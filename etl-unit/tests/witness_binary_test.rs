//! Witness tests for observing ETL transformations
//!
//! Run with: cargo test --test witness_binary_test -- --nocapture
//!
//! These tests use eprintln! to show transformations step-by-step.

use etl_unit::{
	CanonicalColumnName, ColumnNameExt, EtlSchema, EtlUnitSubsetRequest, MeasurementKind,
	source::{BoundSource, EtlUniverseBuildPlan},
	universe::UniverseBuilder,
};
use polars::prelude::*;

/// Helper to print a header line
fn print_header(title: &str) {
	eprintln!("\n{}", "=".repeat(70));
	eprintln!("WITNESS: {}", title);
	eprintln!("{}\n", "=".repeat(70));
}

/// Helper to print a footer line
fn print_footer() {
	eprintln!("{}", "=".repeat(70));
	eprintln!("END WITNESS");
	eprintln!("{}\n", "=".repeat(70));
}

/// Diagnostic function to print all relevant info about source and schema
fn diagnose_source_schema(source: &BoundSource, schema: &EtlSchema) {
	eprintln!("\n--- DIAGNOSTIC INFO ---");

	// Source DataFrame columns
	eprintln!("Source '{}' DataFrame columns:", source.name);
	for col_name in source.data.get_column_names() {
		eprintln!("  - '{}'", col_name);
	}

	// Source column mappings
	eprintln!("\nSource column mappings (canonical -> source):");
	for (canonical, binding) in &source.columns {
		eprintln!("  '{}' -> {:?}", canonical.as_str(), binding);
	}

	// Schema canonical names
	eprintln!("\nSchema canonical names:");
	eprintln!("  subject: '{}'", schema.subject.as_str());
	eprintln!("  time: '{}'", schema.time.as_str());

	eprintln!("\nSchema measurements:");
	for m in &schema.measurements {
		eprintln!(
			"  - name: '{}', value: '{}', kind: {:?}",
			m.name.as_str(),
			m.value.as_str(),
			m.kind
		);
		eprintln!(
			"    components: {:?}",
			m.components.iter().map(|c| c.as_str()).collect::<Vec<_>>()
		);
	}

	// Check can_provide for each measurement
	eprintln!("\ncan_provide checks:");
	eprintln!("  subject '{}': {}", schema.subject.as_str(), source.can_provide(&schema.subject));
	eprintln!("  time '{}': {}", schema.time.as_str(), source.can_provide(&schema.time));

	for m in &schema.measurements {
		let can_provide_name = source.can_provide(&m.name);
		let can_provide_value = source.can_provide(&m.value);
		eprintln!("  measurement name '{}': {}", m.name.as_str(), can_provide_name);
		eprintln!("  measurement value '{}': {}", m.value.as_str(), can_provide_value);

		for comp in &m.components {
			eprintln!("  component '{}': {}", comp.as_str(), source.can_provide(comp));
		}
	}

	// Check get_source_column for each
	eprintln!("\nget_source_column checks:");
	if let Some(src) = source.get_source_column(&schema.subject) {
		eprintln!("  subject '{}' -> source '{}'", schema.subject.as_str(), src.as_str());
	} else {
		eprintln!("  subject '{}' -> NONE", schema.subject.as_str());
	}

	if let Some(src) = source.get_source_column(&schema.time) {
		eprintln!("  time '{}' -> source '{}'", schema.time.as_str(), src.as_str());
	} else {
		eprintln!("  time '{}' -> NONE", schema.time.as_str());
	}

	for m in &schema.measurements {
		if let Some(src) = source.get_source_column(&m.value) {
			eprintln!("  measurement value '{}' -> source '{}'", m.value.as_str(), src.as_str());
		} else {
			eprintln!(
				"  measurement value '{}' -> NONE (this would cause extraction to skip!)",
				m.value.as_str()
			);
		}

		for comp in &m.components {
			if let Some(src) = source.get_source_column(comp) {
				eprintln!("  component '{}' -> source '{}'", comp.as_str(), src.as_str());
			} else {
				eprintln!("  component '{}' -> NONE", comp.as_str());
			}
		}
	}

	eprintln!("--- END DIAGNOSTIC ---\n");
}

/*
/// Basic diagnostic test to understand the extraction flow
#[test]
fn diagnose_extraction_flow() {
	print_header("Extraction Flow Diagnostic");

	// Simple source data
	let source_df = df! {
		 "station" => ["A", "B"],
		 "timestamp" => [100i64, 200],
		 "engine" => ["1", "2"],
		 "status" => ["on", "off"]
	}
	.unwrap();

	eprintln!("Source DataFrame:");
	eprintln!("{}\n", source_df);

	// Schema expects "engine_status" as measurement name
	let schema = EtlSchema::new("test")
		.subject("station")
		.time("timestamp")
		.measurement("engine_status", MeasurementKind::Binary)
		.with_component("engine")
		.with_true_values(["on"])
		.with_false_values(["off"])
		.build()
		.expect("Schema should build");

	eprintln!("Schema built successfully");

	// Try different BoundSource configurations

	// Config 1: Using .map()
	eprintln!("\n=== CONFIG 1: Using .map() ===");
	let source1 = BoundSource::new("telemetry", source_df.clone())
		.map("station".canonical(), "station".source())
		.map("timestamp".canonical(), "timestamp".source())
		.map("engine".canonical(), "engine".source())
		.map("engine_status".canonical(), "status".source());

	diagnose_source_schema(&source1, &schema);

	// Config 2: Using identity (requires matching column names)
	eprintln!("\n=== CONFIG 2: What identity would need ===");
	let identity_df = df! {
		 "station" => ["A", "B"],
		 "timestamp" => [100i64, 200],
		 "engine" => ["1", "2"],
		 "engine_status" => ["on", "off"]  // Column name matches canonical name
	}
	.unwrap();

	let source2 = BoundSource::identity("telemetry", identity_df, &schema);
	diagnose_source_schema(&source2, &schema);

	// Try building with identity config
	eprintln!("\n=== Attempting build with identity config ===");
	let plan2 = EtlUniverseBuildPlan::new(schema.clone()).source(source2);
	match UniverseBuilder::build(&plan2) {
		Ok(universe) => {
			eprintln!("SUCCESS! Universe built:");
			eprintln!("{}", universe.dataframe());
		}
		Err(e) => {
			eprintln!("FAILED: {:?}", e);
		}
	}

	print_footer();
}

/// Witness the Binary measurement truth mapping transformation.
#[test]
fn witness_binary_truth_mapping() {
	print_header("Binary Truth Mapping Transformation");

	// Use column names that MATCH canonical names for identity mapping
	let source_df = df! {
		 "station" => ["Station_A", "Station_A", "Station_A", "Station_A", "Station_A",
							"Station_B", "Station_B", "Station_B", "Station_B", "Station_B"],
		 "timestamp" => [100i64, 100, 100, 200, 200,
							  100, 100, 100, 200, 200],
		 "engine" => ["1", "2", "3", "1", "2",
						  "1", "2", "3", "1", "2"],
		 "engine_status" => ["on", "off", "off", "on", "on",
									"off", "off", "off", "on", "off"]
	}
	.unwrap();

	eprintln!("STEP 1: Source DataFrame (raw string values)");
	eprintln!("{}\n", source_df);

	eprintln!("STEP 2: Schema Definition");
	eprintln!("  - Measurement: engine_status (Binary)");
	eprintln!("  - Component: engine");
	eprintln!("  - True values: [\"on\", \"running\", \"1\"]");
	eprintln!("  - False values: [\"off\", \"stopped\", \"0\"]");
	eprintln!();

	let schema = EtlSchema::new("engine_witness")
		.subject("station")
		.time("timestamp")
		.measurement("engine_status", MeasurementKind::Binary)
		.with_component("engine")
		.with_true_values(["on", "running", "1"])
		.with_false_values(["off", "stopped", "0"])
		.build()
		.expect("Schema should build");

	let measurement = schema.get_measurement("engine_status").unwrap();
	eprintln!("  Measurement kind: {:?}", measurement.kind);
	eprintln!("  Default aggregation: {:?}", measurement.kind.default_aggregation());

	if let Some(ref mapping) = measurement.truth_mapping {
		eprintln!("  Truth mapping:");
		eprintln!("    true_values: {:?}", mapping.true_values);
		eprintln!("    false_values: {:?}", mapping.false_values);
	}
	eprintln!();

	eprintln!("STEP 3: Build Universe (extraction applies truth mapping)");

	// Use identity mapping - source columns already match canonical names
	let source = BoundSource::identity("telemetry", source_df, &schema);

	// Run diagnostic
	diagnose_source_schema(&source, &schema);

	let plan = EtlUniverseBuildPlan::new(schema.clone()).source(source);
	let universe = UniverseBuilder::build(&plan).expect("Universe should build");

	eprintln!("Universe DataFrame (after truth mapping: 'on'->1, 'off'->0):");
	eprintln!("{}\n", universe.dataframe());

	// Rest of test...
	eprintln!("STEP 4a: Subset WITH component (individual engines)");

	let request_with_component = EtlUnitSubsetRequest::new()
		.measurements(vec!["engine_status".into()])
		.include_component("engine");

	let subset_with = universe
		.subset(&request_with_component)
		.expect("Subset should work");

	eprintln!("Request: measurements=[engine_status], include_component(engine)");
	eprintln!("Result (each engine shown separately):");
	eprintln!("{}\n", subset_with.data);

	eprintln!("STEP 4b: Subset WITHOUT component (Any aggregation)");

	let request_crushed = EtlUnitSubsetRequest::new().measurements(vec!["engine_status".into()]);

	let subset_crushed = universe
		.subset(&request_crushed)
		.expect("Subset should work");

	eprintln!("Request: measurements=[engine_status] (no component filter)");
	eprintln!("Result (Any aggregation: 1 if ANY engine is on):");
	eprintln!("{}\n", subset_crushed.data);

	eprintln!("EXPLANATION:");
	eprintln!("  Station_A at time 100: engines [on, off, off] -> max(1,0,0) = 1 (any on)");
	eprintln!("  Station_A at time 200: engines [on, on]       -> max(1,1)   = 1 (any on)");
	eprintln!("  Station_B at time 100: engines [off, off, off]-> max(0,0,0) = 0 (none on)");
	eprintln!("  Station_B at time 200: engines [on, off]      -> max(1,0)   = 1 (any on)");
	eprintln!();

	print_footer();

	// Assertions
	assert_eq!(subset_crushed.data.height(), 4);

	let sorted = subset_crushed
		.data
		.clone()
		.lazy()
		.sort(["station", "timestamp"], SortMultipleOptions::default())
		.collect()
		.unwrap();

	let statuses: Vec<Option<i32>> = sorted
		.column("engine_status")
		.unwrap()
		.i32()
		.unwrap()
		.into_iter()
		.collect();

	assert_eq!(statuses, vec![Some(1), Some(1), Some(0), Some(1)]);
}

/// Witness how different truth mappings affect the same data.
#[test]
fn witness_truth_mapping_variants() {
	print_header("Truth Mapping Variants");

	let source_df = df! {
		 "station" => ["A", "A", "A", "A", "A"],
		 "timestamp" => [100i64, 200, 300, 400, 500],
		 "value" => ["yes", "no", "1", "0", "maybe"]
	}
	.unwrap();

	eprintln!("Source data:");
	eprintln!("{}\n", source_df);

	// Variant 1: Explicit true AND false values
	eprintln!("VARIANT 1: Explicit true AND false values");
	eprintln!("  true_values: [\"yes\", \"1\"]");
	eprintln!("  false_values: [\"no\", \"0\"]");
	eprintln!("  Expected: yes->1, no->0, 1->1, 0->0, maybe->NULL");

	let schema1 = EtlSchema::new("test")
		.subject("station")
		.time("timestamp")
		.measurement("value", MeasurementKind::Binary)
		.with_true_values(["yes", "1"])
		.with_false_values(["no", "0"])
		.build()
		.unwrap();

	let source1 = BoundSource::identity("src", source_df.clone(), &schema1);
	diagnose_source_schema(&source1, &schema1);

	let plan1 = EtlUniverseBuildPlan::new(schema1).source(source1);
	let universe1 = UniverseBuilder::build(&plan1).unwrap();

	eprintln!("Result:");
	eprintln!("{}\n", universe1.dataframe());

	// Variant 2: Only true values (implicit false)
	eprintln!("VARIANT 2: Only true values (everything else is false)");
	eprintln!("  true_values: [\"yes\"]");
	eprintln!("  false_values: (not specified)");
	eprintln!("  Expected: yes->1, everything else->0");

	let schema2 = EtlSchema::new("test")
		.subject("station")
		.time("timestamp")
		.measurement("value", MeasurementKind::Binary)
		.with_true_values(["yes"])
		.build()
		.unwrap();

	let source2 = BoundSource::identity("src", source_df.clone(), &schema2);
	let plan2 = EtlUniverseBuildPlan::new(schema2).source(source2);
	let universe2 = UniverseBuilder::build(&plan2).unwrap();

	eprintln!("Result:");
	eprintln!("{}\n", universe2.dataframe());

	// Variant 3: Numeric default (no mapping specified)
	eprintln!("VARIANT 3: Numeric default (no explicit mapping)");
	eprintln!("  Uses TruthMapping::numeric() -> 1=true, 0=false");

	let numeric_df = df! {
		 "station" => ["A", "A", "A", "A"],
		 "timestamp" => [100i64, 200, 300, 400],
		 "pump_on" => [1i32, 0, 1, 0]
	}
	.unwrap();

	eprintln!("Numeric source data:");
	eprintln!("{}", numeric_df);

	let schema3 = EtlSchema::new("test")
		.subject("station")
		.time("timestamp")
		.measurement("pump_on", MeasurementKind::Binary)
		.build()
		.unwrap();

	let source3 = BoundSource::identity("src", numeric_df, &schema3);
	let plan3 = EtlUniverseBuildPlan::new(schema3).source(source3);
	let universe3 = UniverseBuilder::build(&plan3).unwrap();

	eprintln!("Result:");
	eprintln!("{}\n", universe3.dataframe());

	print_footer();
}

/// Witness the difference between Categorical and Binary for the same data.
#[test]
fn witness_categorical_vs_binary() {
	print_header("Categorical vs Binary Measurement Types");

	// Use column name that matches canonical name
	let source_df = df! {
		 "station" => ["A", "A", "A", "B", "B", "B"],
		 "timestamp" => [100i64, 100, 100, 100, 100, 100],
		 "engine" => ["1", "2", "3", "1", "2", "3"],
		 "engine_status" => ["on", "off", "on", "off", "off", "off"]
	}
	.unwrap();

	eprintln!("Source data:");
	eprintln!("{}\n", source_df);

	// As Categorical
	eprintln!("AS CATEGORICAL:");
	eprintln!("  - Values stay as strings");
	eprintln!("  - Default aggregation: Last");
	eprintln!("  - When crushed: takes last value (arbitrary)");

	let schema_cat = EtlSchema::new("test")
		.subject("station")
		.time("timestamp")
		.measurement("engine_status", MeasurementKind::Categorical)
		.with_component("engine")
		.build()
		.unwrap();

	let source_cat = BoundSource::identity("src", source_df.clone(), &schema_cat);
	diagnose_source_schema(&source_cat, &schema_cat);

	let plan_cat = EtlUniverseBuildPlan::new(schema_cat).source(source_cat);
	let universe_cat = UniverseBuilder::build(&plan_cat).unwrap();

	eprintln!("Universe (categorical, string values preserved):");
	eprintln!("{}", universe_cat.dataframe());

	let subset_cat = universe_cat
		.subset(&EtlUnitSubsetRequest::new().measurements(vec!["engine_status".into()]))
		.unwrap();

	eprintln!("\nCrushed (Last aggregation - takes last value):");
	eprintln!("{}\n", subset_cat.data);

	// As Binary
	eprintln!("AS BINARY:");
	eprintln!("  - Values converted to 0/1 based on truth mapping");
	eprintln!("  - Default aggregation: Any");
	eprintln!("  - When crushed: 1 if ANY engine is on");

	let schema_bin = EtlSchema::new("test")
		.subject("station")
		.time("timestamp")
		.measurement("engine_status", MeasurementKind::Binary)
		.with_component("engine")
		.with_true_values(["on"])
		.with_false_values(["off"])
		.build()
		.unwrap();

	let source_bin = BoundSource::identity("src", source_df, &schema_bin);

	let plan_bin = EtlUniverseBuildPlan::new(schema_bin).source(source_bin);
	let universe_bin = UniverseBuilder::build(&plan_bin).unwrap();

	eprintln!("Universe (binary, values converted to 0/1):");
	eprintln!("{}", universe_bin.dataframe());

	let subset_bin = universe_bin
		.subset(&EtlUnitSubsetRequest::new().measurements(vec!["engine_status".into()]))
		.unwrap();

	eprintln!("\nCrushed (Any aggregation - 1 if any engine on):");
	eprintln!("{}\n", subset_bin.data);

	eprintln!("COMPARISON:");
	eprintln!("  Station_A: engines [on, off, on]");

	let mask_cat_a = subset_cat
		.data
		.column("station")
		.unwrap()
		.str()
		.unwrap()
		.equal("A");
	let filtered_cat_a = subset_cat.data.filter(&mask_cat_a).unwrap();

	eprintln!(
		"    Categorical (Last): '{:?}'",
		filtered_cat_a
			.column("engine_status")
			.unwrap()
			.get(0)
			.unwrap()
	);

	let mask_bin_a = subset_bin
		.data
		.column("station")
		.unwrap()
		.str()
		.unwrap()
		.equal("A");
	let filtered_bin_a = subset_bin.data.filter(&mask_bin_a).unwrap();

	eprintln!(
		"    Binary (Any): {:?} (because 2 engines are on)",
		filtered_bin_a
			.column("engine_status")
			.unwrap()
			.get(0)
			.unwrap()
	);
	eprintln!();

	eprintln!("  Station_B: engines [off, off, off]");

	let mask_cat_b = subset_cat
		.data
		.column("station")
		.unwrap()
		.str()
		.unwrap()
		.equal("B");
	let filtered_cat_b = subset_cat.data.filter(&mask_cat_b).unwrap();

	eprintln!(
		"    Categorical (Last): '{:?}'",
		filtered_cat_b
			.column("engine_status")
			.unwrap()
			.get(0)
			.unwrap()
	);

	let mask_bin_b = subset_bin
		.data
		.column("station")
		.unwrap()
		.str()
		.unwrap()
		.equal("B");
	let filtered_bin_b = subset_bin.data.filter(&mask_bin_b).unwrap();

	eprintln!(
		"    Binary (Any): {:?} (because no engines are on)",
		filtered_bin_b
			.column("engine_status")
			.unwrap()
			.get(0)
			.unwrap()
	);

	print_footer();
} */
