{% macro base_columns() %}
    Campaign_ID,
    Target_Audience,
    Campaign_Goal,
    Duration,
    Channel_Used,
    Conversion_Rate,
    Acquisition_Cost,
    ROI,
    Location,
    Language,
    Clicks,
    Impressions,
    Engagement_Score,
    Customer_Segment,
    Date,
    Company
{% endmacro %}

{% macro derived_metrics() %}
    CAST(Clicks AS FLOAT) / NULLIF(CAST(Impressions AS FLOAT), 0) AS CTR,
    Acquisition_Cost / NULLIF(CAST(Clicks AS FLOAT), 0) AS CPC,
    ROI + 1 AS ROAS,
    Date AS StandardizedDate
{% endmacro %}

{% macro agg_metrics() %}
    COUNT(*) as campaign_count,
    AVG(Conversion_Rate) as avg_conversion_rate,
    AVG(ROI) as avg_roi,
    AVG(Acquisition_Cost) as avg_acquisition_cost,
    SUM(Clicks) as total_clicks,
    SUM(Impressions) as total_impressions,
    CAST(SUM(Clicks) AS FLOAT) / NULLIF(SUM(Impressions), 0) as overall_ctr
{% endmacro %}
