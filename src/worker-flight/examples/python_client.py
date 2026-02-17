#!/usr/bin/env python3
"""
Python client example for testing the Arrow Flight server.

Requirements:
    pip install pyarrow pandas

Usage:
    1. Start the server: cargo run --example basic_server
    2. Run this script: python examples/python_client.py
"""

import pyarrow as pa
import pyarrow.flight as flight
import pandas as pd


def main():
    print("🐍 Python Arrow Flight Client Example\n")

    # Connect to the Flight server
    print("🔌 Connecting to Flight server at grpc://127.0.0.1:8815...")
    client = flight.FlightClient("grpc://127.0.0.1:8815")
    print("✓ Connected\n")

    # List available datasets
    print("📋 Listing available datasets...")
    try:
        for flight_info in client.list_flights():
            print(f"  - {flight_info.descriptor}")
    except Exception as e:
        print(f"  (Error: {e})")
    print()

    # Example 1: Get schema for a query
    print("📝 Getting schema for query...")
    sql_query = "SELECT * FROM sales_data WHERE dt = '2025-01-01'"
    descriptor = flight.FlightDescriptor.for_command(sql_query.encode('utf-8'))
    
    try:
        flight_info = client.get_flight_info(descriptor)
        schema = flight_info.schema
        print(f"✓ Schema fields: {', '.join([f.name for f in schema])}")
        print(f"  Total endpoints: {len(flight_info.endpoints)}")
        print()
    except Exception as e:
        print(f"  Error: {e}\n")
        return

    # Example 2: Execute query and fetch results
    print("🚀 Executing query and fetching results...")
    print(f"   SQL: {sql_query}")
    
    try:
        # Get the ticket from FlightInfo
        ticket = flight_info.endpoints[0].ticket
        
        # Fetch data using the ticket
        reader = client.do_get(ticket)
        
        # Read all batches and convert to pandas
        table = reader.read_all()
        df = table.to_pandas()
        
        print(f"\n✓ Query successful! Retrieved {len(df)} rows\n")
        print("Results:")
        print(df.to_string())
        print()
        
        # Show data types
        print("Data types:")
        print(df.dtypes)
        print()
        
    except Exception as e:
        print(f"  Error: {e}\n")
        return

    # Example 3: Aggregation query
    print("📊 Running aggregation query...")
    agg_query = "SELECT country, SUM(amount) as total_amount FROM sales_data WHERE dt = '2025-01-01' GROUP BY country"
    print(f"   SQL: {agg_query}")
    
    try:
        descriptor = flight.FlightDescriptor.for_command(agg_query.encode('utf-8'))
        flight_info = client.get_flight_info(descriptor)
        ticket = flight_info.endpoints[0].ticket
        reader = client.do_get(ticket)
        table = reader.read_all()
        df = table.to_pandas()
        
        print(f"\n✓ Query successful! Retrieved {len(df)} rows\n")
        print("Results:")
        print(df.to_string())
        print()
        
    except Exception as e:
        print(f"  Error: {e}\n")
        return

    # Example 4: Test health check action
    print("🏥 Testing health check action...")
    try:
        action = flight.Action("health_check", b"")
        result = next(client.do_action(action))
        print(f"✓ Health check: {result.body.to_pybytes().decode('utf-8')}")
        print()
    except Exception as e:
        print(f"  Error: {e}\n")

    print("✨ All examples completed successfully!")


if __name__ == "__main__":
    main()


