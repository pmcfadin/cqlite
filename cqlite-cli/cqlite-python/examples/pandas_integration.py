#!/usr/bin/env python3
"""
🐼 CQLite + Pandas Integration Examples

Demonstrate the seamless integration between CQLite and pandas DataFrames.
This shows how to leverage the world's first SSTable querying library with
pandas for powerful data analysis workflows.
"""

import cqlite
import tempfile
import sys
from pathlib import Path

# Check pandas availability
try:
    import pandas as pd
    import numpy as np
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False


def create_mock_sstable():
    """Create a mock SSTable file for demonstration."""
    temp_dir = tempfile.mkdtemp()
    sstable_path = Path(temp_dir) / "analytics-big-Data.db"
    sstable_path.touch()
    return str(sstable_path)


def example_basic_dataframe_operations():
    """Demonstrate basic DataFrame operations with SSTable data."""
    print("🐼 Example 1: Basic DataFrame Operations")
    print("=" * 50)
    
    if not PANDAS_AVAILABLE:
        print("❌ Pandas not available. Install with: pip install pandas")
        return
    
    sstable_path = create_mock_sstable()
    
    try:
        with cqlite.SSTableReader(sstable_path) as reader:
            # Get DataFrame directly from SSTable query
            print("🔍 Executing: SELECT * FROM analytics")
            df = reader.query_df("SELECT * FROM analytics")
            
            print(f"✅ DataFrame created with shape: {df.shape}")
            print(f"📊 Data types:\n{df.dtypes}")
            print(f"\n📋 First 5 rows:\n{df.head()}")
            
            # Basic DataFrame info
            print(f"\n📈 DataFrame info:")
            print(f"   Memory usage: {df.memory_usage(deep=True).sum()} bytes")
            print(f"   Null values: {df.isnull().sum().sum()}")
            
            # Basic statistics
            if not df.empty:
                print(f"\n📊 Numeric columns statistics:")
                numeric_cols = df.select_dtypes(include=[np.number]).columns
                if len(numeric_cols) > 0:
                    print(df[numeric_cols].describe())
                else:
                    print("   No numeric columns found")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_data_analysis_workflow():
    """Demonstrate a complete data analysis workflow."""
    print("📊 Example 2: Data Analysis Workflow")
    print("=" * 50)
    
    if not PANDAS_AVAILABLE:
        print("❌ Pandas not available. Install with: pip install pandas")
        return
    
    sstable_path = create_mock_sstable()
    
    try:
        with cqlite.SSTableReader(sstable_path) as reader:
            # 1. Load user activity data
            print("📥 Step 1: Loading user activity data...")
            df = reader.query_df("""
                SELECT user_id, event_type, timestamp, session_duration, revenue
                FROM user_events 
                WHERE event_date >= '2023-01-01'
            """)
            
            print(f"   ✅ Loaded {len(df)} events")
            
            # 2. Data cleaning and preprocessing
            print("\n🧹 Step 2: Data cleaning...")
            
            # Remove duplicates
            initial_count = len(df)
            df = df.drop_duplicates()
            print(f"   Removed {initial_count - len(df)} duplicate rows")
            
            # Handle missing values
            missing_count = df.isnull().sum().sum()
            if missing_count > 0:
                print(f"   Found {missing_count} missing values")
                df = df.fillna(0)  # Fill with 0 for demo
                print(f"   Filled missing values with 0")
            
            # Convert data types
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                print("   ✅ Converted timestamp to datetime")
            
            # 3. Exploratory Data Analysis
            print("\n🔍 Step 3: Exploratory Data Analysis...")
            
            if 'event_type' in df.columns:
                event_counts = df['event_type'].value_counts()
                print(f"   📊 Event type distribution:")
                for event, count in event_counts.head().items():
                    print(f"      {event}: {count}")
            
            # Time-based analysis
            if 'timestamp' in df.columns:
                df['hour'] = df['timestamp'].dt.hour
                hourly_activity = df.groupby('hour').size()
                print(f"\n   🕐 Peak activity hour: {hourly_activity.idxmax()}")
                print(f"   🕐 Peak activity count: {hourly_activity.max()}")
            
            # User behavior analysis
            if 'user_id' in df.columns:
                user_stats = df.groupby('user_id').agg({
                    'event_type': 'count',
                    'session_duration': 'mean',
                    'revenue': 'sum'
                }).round(2)
                
                print(f"\n   👥 User statistics (top 5 by events):")
                top_users = user_stats.sort_values('event_type', ascending=False).head()
                print(top_users)
            
            # 4. Advanced aggregations
            print("\n📈 Step 4: Advanced aggregations...")
            
            # Cohort analysis (mock)
            if 'timestamp' in df.columns and 'user_id' in df.columns:
                df['month'] = df['timestamp'].dt.to_period('M')
                monthly_users = df.groupby('month')['user_id'].nunique()
                print(f"   📅 Monthly active users:")
                for month, users in monthly_users.items():
                    print(f"      {month}: {users} users")
            
            # Revenue analysis
            if 'revenue' in df.columns:
                total_revenue = df['revenue'].sum()
                avg_revenue_per_event = df['revenue'].mean()
                print(f"\n   💰 Total revenue: ${total_revenue:,.2f}")
                print(f"   💰 Average revenue per event: ${avg_revenue_per_event:.2f}")
            
            # 5. Data export for further analysis
            print("\n📤 Step 5: Exporting processed data...")
            
            # Save to CSV for external tools
            output_path = "/tmp/processed_analytics.csv"
            df.to_csv(output_path, index=False)
            print(f"   ✅ Exported to: {output_path}")
            
            # Summary report
            print(f"\n📋 Analysis Summary:")
            print(f"   Total events processed: {len(df):,}")
            print(f"   Date range: {df['timestamp'].min()} to {df['timestamp'].max()}" if 'timestamp' in df.columns else "   Date range: N/A")
            print(f"   Unique users: {df['user_id'].nunique()}" if 'user_id' in df.columns else "   Unique users: N/A")
            print(f"   Data quality: {((len(df) - missing_count) / len(df) * 100):.1f}% complete")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_time_series_analysis():
    """Demonstrate time series analysis with SSTable data."""
    print("📈 Example 3: Time Series Analysis")
    print("=" * 50)
    
    if not PANDAS_AVAILABLE:
        print("❌ Pandas not available. Install with: pip install pandas")
        return
    
    sstable_path = create_mock_sstable()
    
    try:
        with cqlite.SSTableReader(sstable_path) as reader:
            # Load time series data
            print("📥 Loading time series data...")
            df = reader.query_df("""
                SELECT timestamp, metric_value, metric_name, host_id
                FROM system_metrics 
                WHERE timestamp >= '2023-01-01'
                ORDER BY timestamp
            """)
            
            print(f"   ✅ Loaded {len(df)} metric points")
            
            # Time series preprocessing
            print("\n⏰ Time series preprocessing...")
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                df = df.set_index('timestamp')
                print("   ✅ Set timestamp as index")
                
                # Resample to hourly averages
                if 'metric_value' in df.columns:
                    hourly_avg = df['metric_value'].resample('H').mean()
                    print(f"   📊 Hourly averages calculated: {len(hourly_avg)} points")
                    
                    # Detect anomalies (simple statistical method)
                    mean_val = hourly_avg.mean()
                    std_val = hourly_avg.std()
                    threshold = 2 * std_val
                    
                    anomalies = hourly_avg[abs(hourly_avg - mean_val) > threshold]
                    print(f"   🚨 Anomalies detected: {len(anomalies)}")
                    
                    if len(anomalies) > 0:
                        print(f"   🚨 Anomaly times: {list(anomalies.index[:5])}")
                
                # Rolling statistics
                if 'metric_value' in df.columns:
                    df['rolling_mean'] = df['metric_value'].rolling(window=24).mean()  # 24-hour rolling average
                    df['rolling_std'] = df['metric_value'].rolling(window=24).std()
                    print("   📊 Rolling statistics calculated")
                
                # Trend analysis
                if len(df) > 1:
                    # Simple linear trend
                    x = np.arange(len(df))
                    if 'metric_value' in df.columns:
                        trend_coef = np.polyfit(x, df['metric_value'].fillna(0), 1)[0]
                        trend_direction = "increasing" if trend_coef > 0 else "decreasing"
                        print(f"   📈 Trend: {trend_direction} (coefficient: {trend_coef:.6f})")
            
            # Host-based analysis
            if 'host_id' in df.columns and 'metric_value' in df.columns:
                print("\n🖥️  Host-based analysis...")
                host_stats = df.groupby('host_id')['metric_value'].agg([
                    'count', 'mean', 'std', 'min', 'max'
                ]).round(2)
                
                print(f"   📊 Stats by host (top 5):")
                print(host_stats.head())
                
                # Find problematic hosts
                high_std_hosts = host_stats[host_stats['std'] > host_stats['std'].quantile(0.9)]
                print(f"   ⚠️  High variability hosts: {len(high_std_hosts)}")
            
            # Correlation analysis
            print("\n🔗 Correlation analysis...")
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if len(numeric_cols) > 1:
                correlation_matrix = df[numeric_cols].corr()
                print(f"   📊 Correlation matrix:")
                print(correlation_matrix.round(3))
                
                # Find highly correlated metrics
                high_corr_pairs = []
                for i in range(len(correlation_matrix.columns)):
                    for j in range(i+1, len(correlation_matrix.columns)):
                        corr_val = correlation_matrix.iloc[i, j]
                        if abs(corr_val) > 0.8:  # High correlation threshold
                            col1, col2 = correlation_matrix.columns[i], correlation_matrix.columns[j]
                            high_corr_pairs.append((col1, col2, corr_val))
                
                if high_corr_pairs:
                    print(f"   🔗 Highly correlated pairs:")
                    for col1, col2, corr in high_corr_pairs:
                        print(f"      {col1} ↔ {col2}: {corr:.3f}")
            
            # Export time series data
            print("\n📤 Exporting time series analysis...")
            
            # Reset index to include timestamp in CSV
            export_df = df.reset_index()
            export_df.to_csv("/tmp/time_series_analysis.csv", index=False)
            print("   ✅ Exported to: /tmp/time_series_analysis.csv")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_large_dataset_processing():
    """Demonstrate efficient processing of large datasets."""
    print("🚀 Example 4: Large Dataset Processing")
    print("=" * 50)
    
    if not PANDAS_AVAILABLE:
        print("❌ Pandas not available. Install with: pip install pandas")
        return
    
    sstable_path = create_mock_sstable()
    
    try:
        with cqlite.SSTableReader(sstable_path) as reader:
            # Chunked processing for large datasets
            print("📦 Chunked processing for large datasets...")
            
            chunk_size = 10000
            total_processed = 0
            aggregated_stats = {}
            
            # Simulate processing large dataset in chunks
            print(f"   🔄 Processing in chunks of {chunk_size:,} rows...")
            
            # In a real scenario, you might use query_async or pagination
            for chunk_num in range(3):  # Simulate 3 chunks
                offset = chunk_num * chunk_size
                
                # Get chunk of data
                chunk_df = reader.query_df(f"""
                    SELECT user_id, event_type, revenue, session_duration
                    FROM large_events_table 
                    LIMIT {chunk_size} OFFSET {offset}
                """)
                
                if chunk_df.empty:
                    break
                
                print(f"      📊 Processing chunk {chunk_num + 1}: {len(chunk_df)} rows")
                
                # Process chunk
                if 'revenue' in chunk_df.columns:
                    chunk_revenue = chunk_df['revenue'].sum()
                    aggregated_stats[f'chunk_{chunk_num}_revenue'] = chunk_revenue
                    print(f"         💰 Chunk revenue: ${chunk_revenue:,.2f}")
                
                if 'user_id' in chunk_df.columns:
                    unique_users = chunk_df['user_id'].nunique()
                    aggregated_stats[f'chunk_{chunk_num}_users'] = unique_users
                    print(f"         👥 Unique users: {unique_users:,}")
                
                total_processed += len(chunk_df)
                
                # Memory management
                del chunk_df  # Free memory
            
            print(f"   ✅ Total rows processed: {total_processed:,}")
            print(f"   📊 Aggregated statistics: {aggregated_stats}")
            
            # Memory-efficient operations
            print("\n💾 Memory-efficient operations...")
            
            # Use categorical data types for string columns
            df = reader.query_df("SELECT * FROM events LIMIT 1000")
            
            if 'event_type' in df.columns:
                memory_before = df.memory_usage(deep=True).sum()
                df['event_type'] = df['event_type'].astype('category')
                memory_after = df.memory_usage(deep=True).sum()
                
                memory_saved = memory_before - memory_after
                percent_saved = (memory_saved / memory_before) * 100
                
                print(f"   📉 Memory optimization:")
                print(f"      Before: {memory_before:,} bytes")
                print(f"      After: {memory_after:,} bytes")
                print(f"      Saved: {memory_saved:,} bytes ({percent_saved:.1f}%)")
            
            # Use data type optimization
            print("\n🔧 Data type optimization...")
            
            # Optimize numeric columns
            for col in df.select_dtypes(include=[np.number]).columns:
                original_dtype = df[col].dtype
                
                # Try to downcast to smaller types
                if df[col].dtype == 'int64':
                    df[col] = pd.to_numeric(df[col], downcast='integer')
                elif df[col].dtype == 'float64':
                    df[col] = pd.to_numeric(df[col], downcast='float')
                
                new_dtype = df[col].dtype
                if original_dtype != new_dtype:
                    print(f"      {col}: {original_dtype} → {new_dtype}")
            
            # Parallel processing simulation
            print("\n⚡ Parallel processing capabilities...")
            
            # Show how CQLite can work with multiprocessing
            print("   🔄 CQLite supports:")
            print("      • Async query execution")
            print("      • Streaming large datasets")
            print("      • Memory-efficient chunked processing")
            print("      • Integration with Dask for distributed computing")
            
            # Export optimized dataset
            optimized_size = df.memory_usage(deep=True).sum()
            print(f"\n📤 Final optimized dataset: {optimized_size:,} bytes")
            
            df.to_parquet("/tmp/optimized_data.parquet", compression='snappy')
            print("   ✅ Exported to: /tmp/optimized_data.parquet")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def example_visualization_prep():
    """Demonstrate preparing data for visualization."""
    print("📊 Example 5: Visualization Data Preparation")
    print("=" * 50)
    
    if not PANDAS_AVAILABLE:
        print("❌ Pandas not available. Install with: pip install pandas")
        return
    
    sstable_path = create_mock_sstable()
    
    try:
        with cqlite.SSTableReader(sstable_path) as reader:
            # Load dashboard data
            print("📈 Preparing data for dashboards...")
            
            df = reader.query_df("""
                SELECT timestamp, user_id, page_views, session_duration, 
                       revenue, country, device_type
                FROM web_analytics 
                WHERE timestamp >= '2023-01-01'
            """)
            
            print(f"   ✅ Loaded {len(df)} analytics records")
            
            # Prepare time series data for charts
            print("\n📅 Time series aggregations...")
            
            if 'timestamp' in df.columns:
                df['timestamp'] = pd.to_datetime(df['timestamp'])
                
                # Daily aggregations
                daily_stats = df.groupby(df['timestamp'].dt.date).agg({
                    'user_id': 'nunique',
                    'page_views': 'sum', 
                    'session_duration': 'mean',
                    'revenue': 'sum'
                }).round(2)
                
                daily_stats.columns = ['unique_users', 'total_page_views', 'avg_session_duration', 'total_revenue']
                print(f"      📊 Daily stats (last 5 days):")
                print(daily_stats.tail())
                
                # Export for time series charts
                daily_stats.to_csv("/tmp/daily_analytics.csv")
                print("      ✅ Exported daily stats")
            
            # Prepare geographic data
            print("\n🌍 Geographic aggregations...")
            
            if 'country' in df.columns:
                country_stats = df.groupby('country').agg({
                    'user_id': 'nunique',
                    'revenue': 'sum',
                    'session_duration': 'mean'
                }).round(2)
                
                country_stats.columns = ['users', 'revenue', 'avg_session_duration']
                country_stats = country_stats.sort_values('revenue', ascending=False)
                
                print(f"      🌍 Top countries by revenue:")
                print(country_stats.head())
                
                # Export for map visualization
                country_stats.to_csv("/tmp/country_analytics.csv")
                print("      ✅ Exported country stats")
            
            # Prepare device/demographic data
            print("\n📱 Device and demographic breakdowns...")
            
            if 'device_type' in df.columns:
                device_stats = df.groupby('device_type').agg({
                    'user_id': 'nunique',
                    'session_duration': 'mean',
                    'page_views': 'mean'
                }).round(2)
                
                print(f"      📱 Device usage stats:")
                print(device_stats)
                
                # Create percentage breakdown for pie charts
                device_users = df.groupby('device_type')['user_id'].nunique()
                device_percentages = (device_users / device_users.sum() * 100).round(1)
                
                print(f"\n      📊 Device usage percentages:")
                for device, pct in device_percentages.items():
                    print(f"         {device}: {pct}%")
                
                # Export for pie/donut charts
                device_percentages.to_csv("/tmp/device_breakdown.csv")
                print("      ✅ Exported device breakdown")
            
            # Prepare funnel analysis data
            print("\n🔄 Funnel analysis preparation...")
            
            # Create conversion funnel
            funnel_data = {
                'stage': ['Landing Page', 'Product View', 'Add to Cart', 'Checkout', 'Purchase'],
                'users': [100, 75, 45, 30, 15],  # Mock funnel data
                'conversion_rate': [100.0, 75.0, 60.0, 66.7, 50.0]
            }
            
            funnel_df = pd.DataFrame(funnel_data)
            print(f"      🔄 Conversion funnel:")
            print(funnel_df)
            
            funnel_df.to_csv("/tmp/conversion_funnel.csv", index=False)
            print("      ✅ Exported funnel data")
            
            # Prepare cohort analysis data
            print("\n👥 Cohort analysis preparation...")
            
            if 'timestamp' in df.columns and 'user_id' in df.columns:
                # Create user cohorts by month
                df['cohort_month'] = df['timestamp'].dt.to_period('M')
                
                cohort_data = df.groupby('cohort_month')['user_id'].nunique()
                retention_data = cohort_data.pct_change().fillna(0) * 100
                
                print(f"      👥 Monthly cohorts:")
                for month, users in cohort_data.items():
                    retention = retention_data[month]
                    print(f"         {month}: {users} users ({retention:+.1f}% change)")
                
                # Export cohort data
                cohort_export = pd.DataFrame({
                    'month': cohort_data.index.astype(str),
                    'users': cohort_data.values,
                    'retention_change': retention_data.values
                })
                
                cohort_export.to_csv("/tmp/cohort_analysis.csv", index=False)
                print("      ✅ Exported cohort data")
            
            # Summary of visualization-ready datasets
            print(f"\n📊 Visualization-ready datasets created:")
            viz_files = [
                "/tmp/daily_analytics.csv",
                "/tmp/country_analytics.csv", 
                "/tmp/device_breakdown.csv",
                "/tmp/conversion_funnel.csv",
                "/tmp/cohort_analysis.csv"
            ]
            
            for file_path in viz_files:
                if Path(file_path).exists():
                    size = Path(file_path).stat().st_size
                    print(f"   ✅ {Path(file_path).name}: {size} bytes")
            
            print(f"\n💡 These datasets are ready for:")
            print(f"   📈 Time series charts (daily_analytics.csv)")
            print(f"   🗺️  Geographic maps (country_analytics.csv)")
            print(f"   🥧 Pie charts (device_breakdown.csv)")
            print(f"   🔄 Funnel charts (conversion_funnel.csv)")
            print(f"   👥 Cohort heatmaps (cohort_analysis.csv)")
    
    except Exception as e:
        print(f"❌ Error (expected with mock data): {e}")
    
    print("\n" + "=" * 50 + "\n")


def main():
    """Run all pandas integration examples."""
    print("🐼 CQLite + Pandas Integration Examples")
    print("🚀 Revolutionary SSTable Querying meets Powerful Data Analysis!")
    print("=" * 70)
    print()
    
    # Check pandas availability
    if PANDAS_AVAILABLE:
        print(f"✅ Pandas {pd.__version__} available")
        print(f"✅ NumPy {np.__version__} available")
    else:
        print("❌ Pandas not available")
        print("💡 Install with: pip install pandas numpy")
    
    print()
    
    # Run examples
    example_basic_dataframe_operations()
    example_data_analysis_workflow()
    example_time_series_analysis() 
    example_large_dataset_processing()
    example_visualization_prep()
    
    print("🎉 All pandas integration examples completed!")
    print("\n💡 Key takeaways:")
    print("   • CQLite seamlessly integrates with pandas DataFrames")
    print("   • Direct SSTable querying enables efficient data pipelines")
    print("   • Memory-efficient processing of large Cassandra datasets")
    print("   • Ready-to-use data for visualization and machine learning")
    print("\n🔗 Next steps:")
    print("   • Try with your actual Cassandra SSTable files")
    print("   • Integrate with Jupyter notebooks for interactive analysis")
    print("   • Build automated data pipelines")
    print("   • Connect to visualization tools like Plotly, Seaborn, or Tableau")


if __name__ == "__main__":
    main()