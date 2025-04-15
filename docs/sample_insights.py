import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import LinearRegression

def generate_insights(company_df: pd.DataFrame, target_col: str = "ROI"):
    insights = {}

    # 1. Correlations
    numerical_cols = company_df.select_dtypes(include=np.number).columns.drop(target_col, errors="ignore")
    correlations = company_df[numerical_cols].corrwith(company_df[target_col]).dropna().sort_values(ascending=False)
    insights['top_correlations'] = correlations.head(3).to_dict()

    # 2. Anomaly detection
    try:
        anomaly_model = IsolationForest(contamination=0.1)
        filtered = company_df[numerical_cols].dropna()
        preds = anomaly_model.fit_predict(filtered)
        anomaly_score = (preds == -1).mean()
        insights['anomaly_ratio'] = anomaly_score
    except Exception as e:
        insights['anomaly_error'] = str(e)

    # 3. Top performing channel
    if 'Channel_Used' in company_df.columns:
        top_channel = company_df.groupby('Channel_Used')[target_col].mean().sort_values(ascending=False).head(1)
        insights['top_channel'] = top_channel.to_dict()

    # 4. Trend in Engagement
    if 'Date' in company_df.columns and 'Engagement_Score' in company_df.columns:
        df = company_df.copy()
        df["Date"] = pd.to_datetime(df["Date"], errors='coerce')
        df = df.dropna(subset=["Date"])
        df["Month"] = df["Date"].dt.to_period("M")
        trend = df.groupby("Month")["Engagement_Score"].mean()
        if len(trend) >= 2:
            X = np.arange(len(trend)).reshape(-1, 1)
            y = trend.values.reshape(-1, 1)
            lr = LinearRegression().fit(X, y)
            slope = lr.coef_[0][0]
            insights["engagement_trend"] = "increasing" if slope > 0 else "decreasing"
    
    return insights
