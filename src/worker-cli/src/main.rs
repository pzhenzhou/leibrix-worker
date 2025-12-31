mod config;

use clap::Parser;

fn main() -> anyhow::Result<()> {
    let cli = config::Cli::parse();

    match cli.command {
        config::Command::Run(args) => {
            let _cfg = config::LeibrixWorkerConfig::from_run_args(args)?;

            // NOTE: a full worker runtime (control-plane stream + data loading coordinator +
            // query service) is not wired up yet in this workspace. Keep behavior local to
            // worker-cli by not starting any long-running services here.
            println!("Hello, Worker CLI!");
        }
    }

    Ok(())
}
