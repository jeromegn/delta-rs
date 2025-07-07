use crate::table::state::DeltaTableState;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::{col, when, Expr, ExprSchemable};
use datafusion::prelude::lit;
use datafusion::{execution::SessionState, prelude::DataFrame};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use tracing::debug;

use crate::{kernel::DataCheck, table::GeneratedColumn, DeltaResult};

/// check if the writer version is able to write generated columns
pub fn able_to_gc(snapshot: &DeltaTableState) -> DeltaResult<bool> {
    if let Some(features) = &snapshot.protocol().writer_features {
        if snapshot.protocol().min_writer_version < 4 {
            return Ok(false);
        }
        if snapshot.protocol().min_writer_version == 7
            && !features.contains(&delta_kernel::table_features::WriterFeature::GeneratedColumns)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Add generated column expressions to a dataframe
pub fn add_missing_generated_columns(
    mut df: DataFrame,
    generated_cols: &[GeneratedColumn],
) -> DeltaResult<DataFrame> {
    for generated_col in generated_cols {
        let col_name = generated_col.get_name();

        if df.schema().field_with_unqualified_name(col_name).is_err()
        // implies it doesn't exist
        {
            debug!("Adding missing generated column {col_name} in source as placeholder");
            // If column doesn't exist, we add a null column, later we will generate the values after
            // all the merge is projected.
            // Other generated columns that were provided upon the start we only validate during write
            df = df.with_column(col_name, lit(ScalarValue::Null))?;
        }
    }
    Ok(df)
}

/// Add generated column expressions to a dataframe
pub fn add_generated_columns(
    mut df: DataFrame,
    generated_cols: &[GeneratedColumn],
    state: &SessionState,
) -> DeltaResult<DataFrame> {
    debug!("Generating columns in dataframe");
    // prevent excessive cloning;
    let mut schema = None;
    for generated_col in generated_cols {
        let generation_expr =
            state.create_logical_expr(generated_col.get_generation_expression(), df.schema())?;
        let col_name = generated_col.get_name();

        schema.replace(df.schema().clone());

        df = df.with_column(
            generated_col.get_name(),
            when(col(col_name).is_null(), generation_expr)
                .otherwise(col(col_name))?
                .cast_to(
                    &((&generated_col.data_type).try_into_arrow()?),
                    schema.as_ref().unwrap(),
                )?,
        )?
    }
    Ok(df)
}
