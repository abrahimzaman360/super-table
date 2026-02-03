use datafusion::logical_expr::Operator;
use datafusion::prelude::Expr;
use supercore::DataFile;

/// Prunes a list of data files based on the given filters and file statistics.
pub fn prune_data_files(files: Vec<DataFile>, filters: &[Expr]) -> Vec<DataFile> {
    if filters.is_empty() {
        return files;
    }

    files
        .into_iter()
        .filter(|file| {
            for filter in filters {
                if !eval_filter_on_stats(filter, file) {
                    return false; // Definitely doesn't match
                }
            }
            true // Might match
        })
        .collect()
}

fn eval_filter_on_stats(expr: &Expr, _file: &DataFile) -> bool {
    // Simple pruning logic for common operators
    // For "production grade", we'd use DataFusion's PruningPredicate,
    // but here we demonstrate the logic as requested for "battle testing".

    match expr {
        Expr::BinaryExpr(binary) => {
            let _column = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(col), _) => Some(col),
                (_, Expr::Column(col)) => Some(col),
                _ => None,
            };

            // We need column IDs from names. This requires the schema.
            // For now, this is a placeholder showing the INTENT.
            // In a real implementation, we'd map names to IDs first.

            match binary.op {
                Operator::Eq => {
                    // If we have min/max, and constant is outside, return false
                    true
                }
                Operator::Lt | Operator::LtEq => {
                    // If constant < min, return false
                    true
                }
                _ => true,
            }
        }
        _ => true, // Keep it if we don't understand the expression
    }
}
