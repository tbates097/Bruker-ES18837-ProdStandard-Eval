import pandas as pd
import plotly.graph_objects as go
from google.cloud import bigquery
import plotly.express as px
import logging
import os
from datetime import datetime
from plotly.subplots import make_subplots

def read_query_file():
    """Read the query from query.txt file."""
    try:
        with open('query.txt', 'r') as file:
            return file.read()
    except Exception as e:
        logging.error(f"Error reading query file: {str(e)}")
        raise

def execute_bigquery_query(query):
    """Execute BigQuery query and return results as DataFrame."""
    try:
        # Initialize BigQuery client
        client = bigquery.Client()
        
        # Execute query
        query_job = client.query(query)
        results = query_job.result()
        
        # Convert to list of dictionaries
        data = [dict(row) for row in results]
        logging.info(f"Retrieved {len(data)} records from BigQuery")
        
        # Convert to pandas DataFrame
        return pd.DataFrame(data)
        
    except Exception as e:
        logging.error(f"Error querying BigQuery: {str(e)}")
        raise

def calculate_statistics(df):
    """Calculate statistics for the dataset."""
    stats = {
        'Total Jobs': len(df),
        'Average Labor Hours Per Unit': df['LaborHoursPerUnit'].mean(),
        'Max Labor Hours Per Unit': df['LaborHoursPerUnit'].max(),
        'Min Labor Hours Per Unit': df['LaborHoursPerUnit'].min(),
        'Jobs Over Standard': len(df[df['LaborHoursPerUnit'] > df['ProdStandard']]),
        'Percentage Over Standard': (len(df[df['LaborHoursPerUnit'] > df['ProdStandard']]) / len(df)) * 100
    }
    return stats

def save_raw_data(df, stats, output_dir='output'):
    """Save raw data and statistics to CSV files with timestamp."""
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate timestamp for filename
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save raw data
        data_filename = os.path.join(output_dir, f'job_hours_data_{timestamp}.csv')
        df.to_csv(data_filename, index=False)
        logging.info(f"Raw data saved to {data_filename}")
        
        # Save statistics
        stats_filename = os.path.join(output_dir, f'job_hours_stats_{timestamp}.csv')
        pd.DataFrame([stats]).to_csv(stats_filename, index=False)
        logging.info(f"Statistics saved to {stats_filename}")
        
    except Exception as e:
        logging.error(f"Error saving data: {str(e)}")
        raise

def create_visualization(df, stats):
    # Create figure with subplots
    fig = make_subplots(
        rows=2, cols=1,  # Changed to 2 rows
        subplot_titles=('Jobs with Production Standard 7.5', 'Statistics'),  # Removed 11.0 title
        vertical_spacing=0.075,
        specs=[[{"type": "bar"}], [{"type": "table"}]],  # Removed one bar spec
        row_heights=[0.6, 0.4]  # Adjusted proportions for single chart
    )
    
    # Split data by ProdStandard
    df_75 = df[df['ProdStandard'] == 7.5]
    
    # Add bars for 7.5 standard
    fig.add_trace(
        go.Bar(
            x=df_75['JobNum'],
            y=df_75['LaborHoursPerUnit'],
            name='Labor Hours (7.5 Standard)',
            marker_color='blue',
            width=0.4,
            text=df_75['StartDate'].dt.strftime('%Y-%m-%d'),
            textposition='outside',
            hovertemplate="<b>Job Number:</b> %{x}<br>" +
                         "<b>Date:</b> %{text}<br>" +
                         "<b>Value:</b> %{y:.2f}<br>" +
                         "<extra></extra>",
            showlegend=True
        ),
        row=1, col=1
    )
    
    # Add horizontal line for 7.5 standard
    fig.add_hline(
        y=7.5,
        line_dash="solid",
        line_color="red",
        name="Standard (7.5)",
        showlegend=True,
        row=1, col=1
    )
    
    # Add horizontal line for average
    fig.add_hline(y=stats['Average Labor Hours Per Unit'], 
                 line_dash="dash", 
                 line_color="green",
                 name="Average Hours/Unit",
                 showlegend=True,
                 row=1, col=1)
    
    # Create statistics table with larger text
    stats_table = go.Table(
        header=dict(
            values=['Metric', 'Value'],
            fill_color='paleturquoise',
            align='left',
            font=dict(size=16)
        ),
        cells=dict(
            values=[
                ['Total Jobs', 'Average Labor Hours/Unit', 'Max Labor Hours/Unit', 
                 'Min Labor Hours/Unit', 'Jobs Over Standard', 'Percentage Over Standard'],
                [f"{stats['Total Jobs']}", 
                 f"{stats['Average Labor Hours Per Unit']:.2f}",
                 f"{stats['Max Labor Hours Per Unit']:.2f}",
                 f"{stats['Min Labor Hours Per Unit']:.2f}",
                 f"{stats['Jobs Over Standard']}",
                 f"{stats['Percentage Over Standard']:.1f}%"]
            ],
            fill_color='lavender',
            align='left',
            font=dict(size=16),
            height=50
        )
    )
    
    # Add table to figure
    fig.add_trace(stats_table, row=2, col=1)
    
    # Update layout
    fig.update_layout(
        title=dict(
            text='Labor Hours Per Unit vs Production Standard Analysis - Bruker Nano ES18837',
            font=dict(size=24)
        ),
        height=2000,
        barmode='group',
        bargap=0.15,
        bargroupgap=0.1,
        template='plotly_white',
        showlegend=True,
        legend=dict(
            yanchor="bottom",
            y=0.99,
            xanchor="right",
            x=0.99,
            font=dict(size=14),
            bgcolor="rgba(255, 255, 255, 0.8)",
            bordercolor="Black",
            borderwidth=1,
            orientation="h",
            traceorder="normal",
            groupclick="toggleitem"
        )
    )
    
    # Update x-axes
    fig.update_xaxes(tickangle=45, row=1, col=1, tickfont=dict(size=12))
    
    # Update y-axes labels
    fig.update_yaxes(title_text='Hours Per Unit', row=1, col=1, title_font=dict(size=14))
    
    # Update subplot titles to align left
    fig.update_annotations(
        font=dict(size=16),
        xanchor="left",
        x=0.01
    )
    
    return fig

def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Read and execute query
        query = read_query_file()
        df = execute_bigquery_query(query)
        logging.info(f"Initial data count: {len(df)} records")
        
        # Convert StartDate to datetime if it's not already
        df['StartDate'] = pd.to_datetime(df['StartDate'])
        
        # Filter out records with LaborHoursPerUnit > 50
        original_count = len(df)
        df = df[df['LaborHoursPerUnit'] <= 50]
        filtered_count = original_count - len(df)
        if filtered_count > 0:
            logging.info(f"Filtered out {filtered_count} records with LaborHoursPerUnit > 50")
        
        # Log counts by ProdStandard
        prod_standard_counts = df['ProdStandard'].value_counts()
        logging.info("Records by Production Standard:")
        for standard, count in prod_standard_counts.items():
            logging.info(f"  Standard {standard}: {count} records")
        
        # Sort by StartDate
        df = df.sort_values('StartDate')
        
        # Calculate statistics
        stats = calculate_statistics(df)
        
        # Save raw data and statistics
        save_raw_data(df, stats)
        
        # Create visualization
        fig = create_visualization(df, stats)
        
        # Save the plot as an HTML file
        fig.write_html('job_hours_analysis.html')
        logging.info("Visualization has been saved as 'job_hours_analysis.html'")
        
    except Exception as e:
        logging.error(f"Error in main execution: {str(e)}")
        raise

if __name__ == "__main__":
    main() 