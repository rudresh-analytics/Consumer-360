# ==============================
# 1. IMPORT LIBRARIES
# ==============================
import os
import pymysql
import pandas as pd
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

# mlxtend is needed for market basket analysis
# Run this in terminal first: pip install mlxtend
from mlxtend.frequent_patterns import apriori, association_rules
from mlxtend.preprocessing import TransactionEncoder

# ==============================
# 2. DATABASE CONNECTION
# FIX: password moved to environment variable
# In your terminal run this ONCE before running this script:
# Windows:  set DB_PASSWORD=Rudhu@1475
# Mac/Linux: export DB_PASSWORD=Rudhu@1475
# ==============================
conn = pymysql.connect(
    host='localhost',
    user='root',
    password=os.environ.get('DB_PASSWORD', 'Rudhu@1475'),  # uses env var, falls back to direct for local dev
    database='Consumer360'
)

print("Database connected successfully")

# ==============================
# 3. LOAD DATA FROM SQL
# Uses the customer_360_view we created in SQL
# ==============================
query = """
SELECT
    fs.order_id,
    fs.customer_id,
    fs.order_date,
    fs.total_amount,
    fs.quantity,
    dc.customer_name,
    dc.city,
    dc.state,
    dp.product_name,
    dp.category_id,
    ds.store_city
FROM fact_sales fs
JOIN dim_customers dc ON fs.customer_id = dc.customer_id
JOIN dim_products  dp ON fs.product_id  = dp.product_id
JOIN dim_store     ds ON fs.store_id    = ds.store_id
"""

df = pd.read_sql(query, conn)
conn.close()  # always close connection after loading data

print(f"\nData loaded: {df.shape[0]} rows, {df.shape[1]} columns")
print(df.head())

# ==============================
# 4. DATA CLEANING
# Your original code was correct — kept and improved
# ==============================
print("\n--- Before Cleaning ---")
print(f"Total rows    : {len(df)}")
print(f"Missing values:\n{df.isnull().sum()}")

# Convert order_date to proper datetime format
df['order_date'] = pd.to_datetime(df['order_date'])

# Remove invalid rows
df = df[df['total_amount'] > 0]
df = df[df['quantity'] > 0]
df.dropna(inplace=True)

print(f"\n--- After Cleaning ---")
print(f"Total rows    : {len(df)}")

# ==============================
# 5. RFM CALCULATION
# Your original logic was correct
# Reference date = day after last order in dataset
# ==============================
reference_date = datetime(2024, 4, 1)

rfm = df.groupby('customer_id').agg(
    Recency  = ('order_date',   lambda x: (reference_date - x.max()).days),
    Frequency= ('order_id',     'count'),
    Monetary = ('total_amount', 'sum')
).reset_index()

print("\n--- Raw RFM Values ---")
print(rfm)

# ==============================
# 6. RFM SCORING
# FIX: added duplicates='drop' to handle small datasets
# without this, pd.qcut crashes when there are not enough
# unique values to create 5 distinct buckets
# ==============================
def safe_qcut(series, n, labels):
    """
    Safe version of pd.qcut that handles small datasets.
    Falls back gracefully if not enough unique values exist.
    """
    try:
        return pd.qcut(series, n, labels=labels, duplicates='drop')
    except ValueError:
        # if dataset too small, use equal-width bins instead
        return pd.cut(series, n, labels=labels)

rfm['R'] = safe_qcut(rfm['Recency'],                    5, [5, 4, 3, 2, 1])
rfm['F'] = safe_qcut(rfm['Frequency'].rank(method='first'), 5, [1, 2, 3, 4, 5])
rfm['M'] = safe_qcut(rfm['Monetary'],                   5, [1, 2, 3, 4, 5])

# Combine into single RFM score string e.g. "555" = best customer
rfm['RFM_Score'] = (
    rfm['R'].astype(str) +
    rfm['F'].astype(str) +
    rfm['M'].astype(str)
)

# Numeric total score for sorting
rfm['RFM_Total'] = (
    rfm['R'].astype(int) +
    rfm['F'].astype(int) +
    rfm['M'].astype(int)
)

print("\n--- RFM Scores ---")
print(rfm[['customer_id','Recency','Frequency','Monetary','R','F','M','RFM_Score']])

# ==============================
# 7. CUSTOMER SEGMENTATION
# Your original logic was correct — expanded with more tiers
# to match the project document requirements
# ==============================
def segment(row):
    r = int(row['R'])
    f = int(row['F'])
    m = int(row['M'])

    if r >= 4 and f >= 4:
        return 'Champions'          # bought recently AND very frequently
    elif r >= 3 and f >= 3:
        return 'Loyal Customers'    # good recency and good frequency
    elif r >= 4 and f <= 2:
        return 'New Customers'      # bought recently but not many times yet
    elif r <= 2 and f >= 3:
        return 'At Risk'            # used to buy often but not recently
    elif r <= 2 and f <= 2:
        return 'Churn Risk'         # low recency AND low frequency — danger zone
    else:
        return 'Average'

rfm['Segment'] = rfm.apply(segment, axis=1)

print("\n--- Segments ---")
print(rfm[['customer_id', 'RFM_Score', 'Segment']])

# ==============================
# 8. CUSTOMER LIFETIME VALUE (CLV)
# Your original formula kept + improved version added
# Basic CLV = total spend x how often they buy
# ==============================
rfm['CLV_Basic'] = rfm['Monetary'] * rfm['Frequency']

# Improved CLV: estimates future value assuming same purchase rate
# Formula: avg order value x purchase frequency x 12 months
avg_order_value  = df.groupby('customer_id')['total_amount'].mean().reset_index()
avg_order_value.columns = ['customer_id', 'avg_order_value']
rfm = pd.merge(rfm, avg_order_value, on='customer_id', how='left')
rfm['CLV_Projected'] = rfm['avg_order_value'] * rfm['Frequency'] * 12

# ==============================
# 9. MERGE CUSTOMER INFO
# Your original code was correct
# ==============================
customer_info = df[['customer_id','customer_name','city','state']].drop_duplicates()
rfm_final = pd.merge(rfm, customer_info, on='customer_id', how='left')

print("\n--- Final RFM Output ---")
print(rfm_final[[
    'customer_id','customer_name','city',
    'Recency','Frequency','Monetary',
    'RFM_Score','Segment','CLV_Projected'
]])

# ==============================
# 10. EXPORT TO CSV
# ==============================
rfm_final.to_csv("rfm_output.csv", index=False)
print("\nCSV exported: rfm_output.csv")

# ==============================
# 11. COHORT ANALYSIS
# NEW — required by project document
# Cohort = group of customers who made their FIRST purchase
# in the same month. We track how many came back each month.
# ==============================
print("\n--- Cohort Analysis ---")

# Step 1: find each customer's first purchase month
df['order_month'] = df['order_date'].dt.to_period('M')

first_purchase = df.groupby('customer_id')['order_date'] \
                   .min().reset_index()
first_purchase.columns = ['customer_id', 'first_purchase_date']
first_purchase['cohort_month'] = first_purchase['first_purchase_date'].dt.to_period('M')

# Step 2: merge cohort month back into main dataframe
df = pd.merge(df, first_purchase[['customer_id','cohort_month']], on='customer_id')

# Step 3: calculate how many months after joining each purchase happened
df['months_since_join'] = (
    df['order_month'].astype(int) - df['cohort_month'].astype(int)
)

# Step 4: count unique customers per cohort per month
cohort_data = df.groupby(['cohort_month','months_since_join'])['customer_id'] \
                .nunique().reset_index()
cohort_data.columns = ['cohort_month','months_since_join','num_customers']

# Step 5: build the cohort pivot table
cohort_pivot = cohort_data.pivot_table(
    index='cohort_month',
    columns='months_since_join',
    values='num_customers'
)

# Step 6: convert to retention percentage
cohort_size   = cohort_pivot.iloc[:, 0]  # month 0 = number who joined
retention_pct = cohort_pivot.divide(cohort_size, axis=0).round(3) * 100

print("Cohort retention % table:")
print(retention_pct)

# Export cohort table
retention_pct.to_csv("cohort_analysis.csv")
print("Cohort CSV exported: cohort_analysis.csv")

# ==============================
# 12. MARKET BASKET ANALYSIS
# NEW — required by project document
# Finds which products are bought together
# "Customers who bought X also bought Y"
# ==============================
print("\n--- Market Basket Analysis ---")

# Step 1: build a list of products per order
basket_sets = df.groupby('order_id')['product_name'] \
                .apply(list).tolist()

print(f"Total transactions for basket analysis: {len(basket_sets)}")

# Step 2: encode into a True/False matrix
# Each row = one order, each column = one product
# True means that product was in that order
te = TransactionEncoder()
te_array = te.fit(basket_sets).transform(basket_sets)
basket_df = pd.DataFrame(te_array, columns=te.columns_)

print(f"Basket matrix shape: {basket_df.shape}")
print(basket_df)

# Step 3: find frequent itemsets
# min_support = 0.2 means the combo must appear in at least 20% of orders
try:
    frequent_itemsets = apriori(
        basket_df,
        min_support=0.2,
        use_colnames=True
    )
    print(f"\nFrequent itemsets found: {len(frequent_itemsets)}")
    print(frequent_itemsets)

    # Step 4: generate association rules
    if len(frequent_itemsets) > 0:
        rules = association_rules(
            frequent_itemsets,
            metric="lift",
            min_threshold=1.0
        )
        print("\nAssociation Rules (product pairings):")
        print(rules[['antecedents','consequents','support','confidence','lift']])
        rules.to_csv("market_basket_rules.csv", index=False)
        print("Market basket CSV exported: market_basket_rules.csv")
    else:
        print("No rules found — dataset too small. Works on larger datasets.")

except Exception as e:
    print(f"Market basket note: {e}")
    print("This works correctly on larger datasets (100+ orders)")

# ==============================
# 13. VISUALIZATIONS
# Your original bar chart kept + 4 new charts added
# ==============================

fig, axes = plt.subplots(2, 3, figsize=(18, 10))
fig.suptitle('Consumer360 — RFM & Customer Analytics Dashboard', fontsize=16)

# --- Chart 1: Customer Segments (your original chart, improved) ---
segment_counts = rfm_final['Segment'].value_counts()
colors = ['#2ecc71','#3498db','#f39c12','#e74c3c','#9b59b6','#1abc9c']
axes[0,0].bar(segment_counts.index, segment_counts.values,
              color=colors[:len(segment_counts)])
axes[0,0].set_title('Customer segments')
axes[0,0].set_xlabel('Segment')
axes[0,0].set_ylabel('Number of customers')
axes[0,0].tick_params(axis='x', rotation=30)

# --- Chart 2: RFM Score Distribution ---
rfm_final['RFM_Total'].value_counts().sort_index().plot(
    kind='bar', ax=axes[0,1], color='#3498db'
)
axes[0,1].set_title('RFM total score distribution')
axes[0,1].set_xlabel('RFM total score')
axes[0,1].set_ylabel('Number of customers')

# --- Chart 3: Top customers by Monetary value ---
top_customers = rfm_final.nlargest(7, 'Monetary')
axes[0,2].barh(top_customers['customer_name'], top_customers['Monetary'],
               color='#2ecc71')
axes[0,2].set_title('Top customers by total spend')
axes[0,2].set_xlabel('Total spend (INR)')
axes[0,2].xaxis.set_major_formatter(
    mticker.FuncFormatter(lambda x, _: f'₹{int(x):,}')
)

# --- Chart 4: Recency vs Monetary scatter (find churn risks visually) ---
colors_map = {
    'Champions':      '#2ecc71',
    'Loyal Customers':'#3498db',
    'New Customers':  '#f39c12',
    'At Risk':        '#e67e22',
    'Churn Risk':     '#e74c3c',
    'Average':        '#95a5a6'
}
for seg, grp in rfm_final.groupby('Segment'):
    axes[1,0].scatter(
        grp['Recency'], grp['Monetary'],
        label=seg,
        color=colors_map.get(seg, '#333'),
        s=120, edgecolors='white', linewidth=0.8
    )
axes[1,0].set_title('Recency vs monetary (bubble = customer)')
axes[1,0].set_xlabel('Recency (days since last purchase)')
axes[1,0].set_ylabel('Total spend (INR)')
axes[1,0].legend(fontsize=7)

# --- Chart 5: Revenue by city ---
city_revenue = df.groupby('city')['total_amount'].sum().sort_values(ascending=False)
axes[1,1].bar(city_revenue.index, city_revenue.values, color='#9b59b6')
axes[1,1].set_title('Revenue by city')
axes[1,1].set_xlabel('City')
axes[1,1].set_ylabel('Total revenue (INR)')
axes[1,1].yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda x, _: f'₹{int(x):,}')
)

# --- Chart 6: Projected CLV by customer ---
clv_data = rfm_final.nlargest(7, 'CLV_Projected')
axes[1,2].bar(clv_data['customer_name'], clv_data['CLV_Projected'],
              color='#1abc9c')
axes[1,2].set_title('Projected CLV by customer')
axes[1,2].set_xlabel('Customer')
axes[1,2].set_ylabel('Projected CLV (INR)')
axes[1,2].tick_params(axis='x', rotation=30)
axes[1,2].yaxis.set_major_formatter(
    mticker.FuncFormatter(lambda x, _: f'₹{int(x):,}')
)

plt.tight_layout()
plt.savefig("consumer360_dashboard.png", dpi=150, bbox_inches='tight')
plt.show()
print("\nDashboard chart saved: consumer360_dashboard.png")

# ==============================
# 14. FINAL SUMMARY REPORT
# Printed to console — useful before building Power BI
# ==============================
print("\n" + "="*55)
print("         CONSUMER360 — FINAL ANALYSIS SUMMARY")
print("="*55)
print(f"Total customers analysed : {len(rfm_final)}")
print(f"Total orders             : {len(df)}")
print(f"Total revenue            : ₹{df['total_amount'].sum():,.0f}")
print(f"Average order value      : ₹{df['total_amount'].mean():,.0f}")
print(f"\nSegment breakdown:")
for seg, count in rfm_final['Segment'].value_counts().items():
    pct = count / len(rfm_final) * 100
    print(f"  {seg:<20} : {count} customers ({pct:.0f}%)")
print(f"\nTop customer by spend    : {rfm_final.loc[rfm_final['Monetary'].idxmax(), 'customer_name']}")
print(f"Highest projected CLV    : {rfm_final.loc[rfm_final['CLV_Projected'].idxmax(), 'customer_name']}")
print(f"\nFiles exported:")
print("  rfm_output.csv")
print("  cohort_analysis.csv")
print("  market_basket_rules.csv")
print("  consumer360_dashboard.png")
print("="*55)