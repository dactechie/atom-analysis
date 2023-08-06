import pandas as pd
import streamlit as st

st.set_page_config(page_title="ATOM Dashboard",
                   page_icon=":bar_chart:",
                   layout="wide")

fname = "./tmp/processed_all_cols_20200101_20240101_1.parquet"
df = pd.read_parquet(fname)



st.sidebar.header("Filters")

all_drugs = df["PDCSubstanceOrGambling"].cat.categories.to_list()
drug = st.sidebar.multiselect(
    "Select the drug:",
    options=df["PDCSubstanceOrGambling"].cat.categories.to_list(),
    default=["Ethanol"]
)

df_selection = df.query(
    "PDCSubstanceOrGambling == @drug"
)
# st.dataframe(df_selection)

# -- main page --------------------
st.title(f":bar_chart: Dashboard for Drug(s) : {drug}")
st.markdown("##")

# Top KPIs

total_atsi  = df['IndigenousStatus'].value_counts()
left , middle, right = st.columns(3)
with middle:
    st.bar_chart(total_atsi)
    # st.subheader(total_atsi)
st.markdown("---")