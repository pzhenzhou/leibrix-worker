//! Stage cutting for LDP plans.
//!
//! This module takes an annotated Substrait plan (with exchanges) and cuts it
//! into separate stages connected by exchange edges.
//!
//! # Algorithm
//! ```text
//! cut_into_stages(annotated):
//!   1. Find all nodes with exchanges (cut points)
//!   2. For each cut point, create a stage for the subtree below the exchange
//!   3. Replace cut subtrees with ReadRel placeholders
//!   4. Create the root stage with remaining operators
//!   5. Return LdpPlan with stages and exchange edges
//! ```

use crate::ldp::planner::annotate::AnnotatedRel;
use crate::ldp::planner::policy::PlannerPolicy;
use crate::ldp::substrait::reconstruct::{
    create_read_rel, exchange_table_name, reconstruct_rel_with_children, serialize_rel_to_substrait,
};
use crate::ldp::{
    Exchange, ExchangeEdge, ExchangeId, LdpPlan, Stage, StageId, StageInput, StageOutput,
    WorkerId,
};
use substrait::proto::Rel;

/// Cut an annotated plan into stages at exchange boundaries.
///
/// This is the main entry point for stage cutting. It takes an annotated plan
/// (output of `annotate_and_enforce`) and produces an LdpPlan with:
/// - Stages: executable Substrait fragments
/// - Edges: exchange connections between stages
///
/// # Arguments
/// * `annotated` - The annotated plan with exchanges marked
/// * `policy` - Planner policy for coordinator info
/// * `query_id` - Unique identifier for this query
///
/// # Returns
/// An LdpPlan ready for execution.
pub fn cut_into_stages(
    annotated: &AnnotatedRel,
    policy: &PlannerPolicy,
    query_id: impl Into<String>,
) -> LdpPlan {
    let mut plan = LdpPlan::new(query_id.into(), policy.coordinator.clone());
    let mut ctx = CutContext::new();

    // Recursively cut the plan, starting from root
    let root_stage_id = cut_recursive(annotated, &mut plan, &mut ctx, policy);

    // Set the root stage
    plan.root_stage = root_stage_id;

    // Assign target workers to stages based on distribution annotations
    assign_stage_workers(&mut plan, annotated, &ctx);

    plan
}

/// Context for stage cutting traversal.
struct CutContext {
    /// Next available stage ID.
    next_stage_id: StageId,
    /// Next available exchange ID.
    next_exchange_id: ExchangeId,
    /// Mapping from stage ID to the annotated node that forms its root.
    stage_annotations: std::collections::HashMap<StageId, AnnotatedRelRef>,
}

/// Reference to an AnnotatedRel for worker assignment.
#[derive(Clone)]
struct AnnotatedRelRef {
    workers: Vec<WorkerId>,
}

impl CutContext {
    fn new() -> Self {
        Self {
            next_stage_id: 0,
            next_exchange_id: 0,
            stage_annotations: std::collections::HashMap::new(),
        }
    }

    fn alloc_stage_id(&mut self) -> StageId {
        let id = self.next_stage_id;
        self.next_stage_id += 1;
        id
    }

    fn alloc_exchange_id(&mut self) -> ExchangeId {
        let id = self.next_exchange_id;
        self.next_exchange_id += 1;
        id
    }
}

/// Recursively cut the annotated plan into stages.
///
/// Returns the stage ID of the stage created for this subtree.
fn cut_recursive(
    annotated: &AnnotatedRel,
    plan: &mut LdpPlan,
    ctx: &mut CutContext,
    policy: &PlannerPolicy,
) -> StageId {
    // Process children first (bottom-up)
    let mut child_stage_ids: Vec<StageId> = Vec::new();
    let mut child_exchange_ids: Vec<Option<ExchangeId>> = Vec::new();
    let mut child_rels_for_stage: Vec<Rel> = Vec::new();

    for child in &annotated.children {
        if child.exchange_before.is_some() {
            // This child has an exchange - cut here
            // Create a stage for the child subtree
            let child_stage_id = cut_recursive(child, plan, ctx, policy);
            child_stage_ids.push(child_stage_id);

            // Create exchange edge
            let exchange_id = ctx.alloc_exchange_id();
            let exchange = child.exchange_before.clone().unwrap();

            // Create a placeholder ReadRel for this exchange input
            let exchange_table = exchange_table_name(exchange_id);
            let placeholder = create_read_rel(&exchange_table, None);
            child_rels_for_stage.push(placeholder);
            child_exchange_ids.push(Some(exchange_id));

            // The edge will be added after we know this stage's ID
            // Store exchange info for later
            plan.edges.push(ExchangeEdge {
                exchange_id,
                kind: exchange,
                from_stage: child_stage_id,
                to_stage: 0, // Will be updated below
                partition_to_worker: vec![],
            });
        } else {
            // No exchange - inline this child's rel directly
            // But we still need to process its subtree for any nested exchanges
            let child_rel = extract_stage_rel(child, plan, ctx, policy);
            child_rels_for_stage.push(child_rel);
            child_exchange_ids.push(None);
        }
    }

    // Create this stage
    let stage_id = ctx.alloc_stage_id();

    // Update exchange edges with correct to_stage
    for edge in &mut plan.edges {
        if edge.to_stage == 0 && child_stage_ids.contains(&edge.from_stage) {
            edge.to_stage = stage_id;
        }
    }

    // Build the stage's Rel by reconstructing with new children
    let stage_rel = reconstruct_rel_with_children(&annotated.rel, child_rels_for_stage);
    let substrait_bytes = serialize_rel_to_substrait(&stage_rel);

    // Determine stage inputs
    let mut inputs = Vec::new();
    let mut has_local_catalog = false;

    for (i, exchange_id_opt) in child_exchange_ids.iter().enumerate() {
        if let Some(exchange_id) = exchange_id_opt {
            inputs.push(StageInput::ExchangeInput {
                exchange_id: *exchange_id,
                table_name: exchange_table_name(*exchange_id),
            });
        } else {
            // This child was inlined - check if it reads from local catalog
            if !has_local_catalog && has_local_read(&annotated.children[i]) {
                has_local_catalog = true;
            }
        }
    }

    // If this stage has any local reads (not from exchanges), add LocalCatalog input
    if has_local_catalog || (inputs.is_empty() && has_local_read(annotated)) {
        inputs.insert(0, StageInput::LocalCatalog);
    }

    // Determine output format based on downstream exchange
    let output = determine_stage_output(annotated);

    // Create the stage
    let stage = Stage {
        stage_id,
        target_workers: vec![], // Will be assigned later
        inputs,
        output,
        substrait_plan: substrait_bytes,
        limits: Default::default(),
    };

    // Record annotation for worker assignment
    ctx.stage_annotations.insert(
        stage_id,
        AnnotatedRelRef {
            workers: annotated.annotation.distribution.workers().to_vec(),
        },
    );

    plan.stages.push(stage);
    stage_id
}

/// Extract a Rel for a stage, handling nested exchanges.
fn extract_stage_rel(
    annotated: &AnnotatedRel,
    plan: &mut LdpPlan,
    ctx: &mut CutContext,
    policy: &PlannerPolicy,
) -> Rel {
    // If this node has no children with exchanges, just return the rel directly
    let has_exchange_children = annotated
        .children
        .iter()
        .any(|c| c.exchange_before.is_some() || has_nested_exchange(c));

    if !has_exchange_children {
        return annotated.rel.clone();
    }

    // Otherwise, we need to process children
    let mut new_children = Vec::new();

    for child in &annotated.children {
        if child.exchange_before.is_some() {
            // Cut here - create a stage for child
            let child_stage_id = cut_recursive(child, plan, ctx, policy);

            // Create exchange edge
            let exchange_id = ctx.alloc_exchange_id();
            let exchange = child.exchange_before.clone().unwrap();

            plan.edges.push(ExchangeEdge {
                exchange_id,
                kind: exchange,
                from_stage: child_stage_id,
                to_stage: 0, // Will be set by caller
                partition_to_worker: vec![],
            });

            // Create placeholder
            let placeholder = create_read_rel(&exchange_table_name(exchange_id), None);
            new_children.push(placeholder);
        } else {
            // Recursively extract
            let child_rel = extract_stage_rel(child, plan, ctx, policy);
            new_children.push(child_rel);
        }
    }

    reconstruct_rel_with_children(&annotated.rel, new_children)
}

/// Check if an annotated rel has any nested exchanges.
fn has_nested_exchange(annotated: &AnnotatedRel) -> bool {
    annotated.exchange_before.is_some()
        || annotated.children.iter().any(has_nested_exchange)
}

/// Check if an annotated rel reads from local catalog (has Read nodes).
fn has_local_read(annotated: &AnnotatedRel) -> bool {
    use substrait::proto::rel::RelType;

    if let Some(RelType::Read(_)) = &annotated.rel.rel_type {
        return true;
    }

    annotated.children.iter().any(has_local_read)
}

/// Determine the output format for a stage.
fn determine_stage_output(annotated: &AnnotatedRel) -> StageOutput {
    // If the annotated node's output distribution is hash partitioned, mark the stage output
    // as partitioned to align with downstream consumers.
    if let crate::ldp::Distribution::HashPartitioned { .. } = annotated.annotation.distribution {
        let partitions = annotated
            .exchange_before
            .as_ref()
            .and_then(|ex| if let Exchange::HashPartition { partitions, .. } = ex {
                Some(*partitions)
            } else {
                None
            })
            .unwrap_or_else(|| policy_default_partitions());

        let field_refs = match &annotated.annotation.distribution {
            crate::ldp::Distribution::HashPartitioned { field_refs, .. } => field_refs.clone(),
            _ => Vec::new(),
        };

        return StageOutput::Partitioned {
            partitions,
            field_refs,
        };
    }

    // If this annotated node has an exchange_before, it will produce partitioned output
    // for hash partition exchanges
    if let Some(exchange) = &annotated.exchange_before {
        if let Exchange::HashPartition { partitions, .. } = exchange {
            return StageOutput::Partitioned {
                    partitions: *partitions,
                    field_refs: match exchange {
                        Exchange::HashPartition { field_refs, .. } => field_refs.clone(),
                        _ => Vec::new(),
                    },
            };
        }
    }

    // Default to stream output
    StageOutput::Stream
}

/// Default partitions helper (matches PlannerPolicy default).
fn policy_default_partitions() -> u32 {
    crate::ldp::planner::policy::PlannerPolicy::default().default_partitions
}

/// Assign target workers to stages based on distribution annotations.
fn assign_stage_workers(
    plan: &mut LdpPlan,
    root_annotated: &AnnotatedRel,
    ctx: &CutContext,
) {
    for stage in &mut plan.stages {
        if let Some(ann_ref) = ctx.stage_annotations.get(&stage.stage_id) {
            stage.target_workers = ann_ref.workers.clone();
        }

        // If no workers assigned (shouldn't happen), use coordinator
        if stage.target_workers.is_empty() {
            stage.target_workers = vec![plan.coordinator.clone()];
        }
    }

    // Also update partition_to_worker for hash partition exchanges
    for edge in &mut plan.edges {
        if let Exchange::HashPartition { partitions, .. } = &edge.kind {
            // For hash partition, assign partitions to available workers
            if let Some(to_stage) = plan.stages.iter().find(|s| s.stage_id == edge.to_stage) {
                let workers = &to_stage.target_workers;
                let mut partition_workers = Vec::with_capacity(*partitions as usize);

                for i in 0..*partitions {
                    // Round-robin assignment
                    let worker = &workers[i as usize % workers.len()];
                    partition_workers.push(worker.clone());
                }

                edge.partition_to_worker = partition_workers;
            }
        }
    }
}

/// Extract only the Substrait Rel from an AnnotatedRel, replacing exchange children with placeholders.
///
/// This is a simpler version for cases where you just need the serialized bytes.
pub fn extract_stage_substrait(annotated: &AnnotatedRel) -> Vec<u8> {
    let rel = extract_rel_with_placeholders(annotated, &mut 0);
    serialize_rel_to_substrait(&rel)
}

/// Extract Rel with exchange children replaced by ReadRel placeholders.
fn extract_rel_with_placeholders(annotated: &AnnotatedRel, exchange_counter: &mut u32) -> Rel {
    if annotated.children.is_empty() {
        return annotated.rel.clone();
    }

    let mut new_children = Vec::new();

    for child in &annotated.children {
        if child.exchange_before.is_some() {
            // Replace with placeholder
            let placeholder = create_read_rel(&exchange_table_name(*exchange_counter), None);
            *exchange_counter += 1;
            new_children.push(placeholder);
        } else {
            // Recursively process
            let child_rel = extract_rel_with_placeholders(child, exchange_counter);
            new_children.push(child_rel);
        }
    }

    reconstruct_rel_with_children(&annotated.rel, new_children)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ldp::planner::annotate::AnnotatedRel;
    use crate::ldp::planner::metadata::InMemoryMetadata;
    use crate::ldp::planner::policy::PlannerPolicy;
    use crate::ldp::{Distribution, DistributionAnnotation, EpochStats, StatsSource};
    use substrait::proto::read_rel::ReadType;
    use substrait::proto::rel::RelType;
    use substrait::proto::{FilterRel, ReadRel, SortRel};

    fn test_policy() -> PlannerPolicy {
        PlannerPolicy::with_coordinator("coordinator")
    }

    fn create_read_annotated(table: &str, workers: Vec<&str>) -> AnnotatedRel {
        let rel = create_read_rel(table, None);
        let workers: Vec<WorkerId> = workers.into_iter().map(String::from).collect();
        let dist = if workers.len() == 1 {
            Distribution::Singleton {
                worker: workers[0].clone(),
            }
        } else {
            Distribution::EpochPartitioned {
                workers: workers.clone(),
            }
        };

        AnnotatedRel::new(
            rel,
            DistributionAnnotation::with_source(dist, 1000, 10000, StatsSource::Exact),
            vec![],
        )
    }

    fn create_filter_annotated(input: AnnotatedRel) -> AnnotatedRel {
        let filter_rel = Rel {
            rel_type: Some(RelType::Filter(Box::new(FilterRel {
                input: Some(Box::new(input.rel.clone())),
                ..Default::default()
            }))),
        };

        AnnotatedRel::new(filter_rel, input.annotation.clone(), vec![input])
    }

    #[test]
    fn test_simple_no_exchange() {
        let policy = test_policy();

        // Simple plan: Read -> Filter (no exchange needed)
        let read = create_read_annotated("sales", vec!["w1"]);
        let filter = create_filter_annotated(read);

        let plan = cut_into_stages(&filter, &policy, "q1");

        // Should be 1 stage, no exchanges
        assert_eq!(plan.stages.len(), 1);
        assert!(plan.edges.is_empty());
        assert_eq!(plan.stages[0].stage_id, 0);
    }

    #[test]
    fn test_with_gather_exchange() {
        let policy = test_policy();

        // Read (distributed) -> Sort (needs gather)
        let mut read = create_read_annotated("sales", vec!["w1", "w2"]);

        // Create sort that needs Singleton
        let sort_rel = Rel {
            rel_type: Some(RelType::Sort(Box::new(SortRel {
                input: Some(Box::new(read.rel.clone())),
                ..Default::default()
            }))),
        };

        // Mark exchange on the read child
        read.exchange_before = Some(Exchange::Gather {
            target: "coordinator".into(),
        });

        // Update read's distribution to reflect post-exchange state
        read.annotation.distribution = Distribution::Singleton {
            worker: "coordinator".into(),
        };

        let sort = AnnotatedRel::new(
            sort_rel,
            DistributionAnnotation::with_source(
                Distribution::Singleton {
                    worker: "coordinator".into(),
                },
                1000,
                10000,
                StatsSource::Exact,
            ),
            vec![read],
        );

        let plan = cut_into_stages(&sort, &policy, "q1");

        // Should be 2 stages with 1 exchange
        assert_eq!(plan.stages.len(), 2, "Expected 2 stages");
        assert_eq!(plan.edges.len(), 1, "Expected 1 exchange edge");

        // Verify exchange edge
        let edge = &plan.edges[0];
        assert!(matches!(edge.kind, Exchange::Gather { .. }));
        assert_eq!(edge.from_stage, 0);
        assert_eq!(edge.to_stage, 1);
    }

    #[test]
    fn test_extract_stage_substrait() {
        let read = create_read_annotated("sales", vec!["w1"]);
        let filter = create_filter_annotated(read);

        let bytes = extract_stage_substrait(&filter);
        assert!(!bytes.is_empty());

        // Should be deserializable
        use crate::ldp::substrait::reconstruct::deserialize_rel_from_substrait;
        let deserialized = deserialize_rel_from_substrait(&bytes);
        assert!(deserialized.is_ok());
    }

    #[test]
    fn test_stage_inputs() {
        let policy = test_policy();
        let read = create_read_annotated("sales", vec!["w1"]);
        let filter = create_filter_annotated(read);

        let plan = cut_into_stages(&filter, &policy, "q1");

        // Single stage should have LocalCatalog input
        assert_eq!(plan.stages[0].inputs.len(), 1);
        assert!(matches!(
            plan.stages[0].inputs[0],
            StageInput::LocalCatalog
        ));
    }

    #[test]
    fn test_stage_output_partitioned_for_hash_exchange() {
        let policy = test_policy();

        // Read distributed across two workers, then hash partition
        let mut read = create_read_annotated("sales", vec!["w1", "w2"]);
        read.exchange_before = Some(Exchange::hash_partition(vec![0], 2));
        read.annotation.distribution = Distribution::HashPartitioned {
            field_refs: vec![0],
            workers: vec!["w1".into(), "w2".into()],
        };

        let filter = create_filter_annotated(read);
        let plan = cut_into_stages(&filter, &policy, "q1");

        // Stage created for the read should be partitioned
        let stage = plan
            .stages
            .iter()
            .find(|s| s.stage_id == 0)
            .expect("stage 0 exists");

        match &stage.output {
            StageOutput::Partitioned { partitions, field_refs } => {
                assert_eq!(*partitions, 2);
                assert_eq!(field_refs, &vec![0]);
            }
            _ => panic!("expected partitioned stage output"),
        }
    }

    #[test]
    fn test_exchange_table_names() {
        assert_eq!(exchange_table_name(0), "__exchange_0");
        assert_eq!(exchange_table_name(42), "__exchange_42");
    }
}
