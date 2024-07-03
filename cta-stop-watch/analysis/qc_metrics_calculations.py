
import polars as pl 
def in_actual_not_schedule(combined_metrics_df):

    actual_columns = [col for col in combined_metrics_df.columns if 'actual' in col]
    schedule_columns = [col for col in combined_metrics_df.columns if 'schedule' in col]

    actual_indices = [combined_metrics_df.columns.index(col) for col in actual_columns]
    schedule_indices = [combined_metrics_df.columns.index(col) for col in schedule_columns]

    # compares null values in actual vs schedule for each row
    def compare_nulls(row):
        actual_nulls = [row[idx] is None for idx in actual_indices]
        schedule_nulls = [row[idx] is None for idx in schedule_indices]
        
        # checks if any actual column is not null and the corresponding schedule column is null
        return any(not a and s for a, s in zip(actual_nulls, schedule_nulls))

    null_comparisons = combined_metrics_df.map_rows(compare_nulls)

    df_with_null_comparisons = combined_metrics_df.with_columns([pl.Series("actual_nulls_not_in_schedule", null_comparisons)])

    return df_with_null_comparisons