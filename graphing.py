import pandas as pd
import altair as alt


def get_chart_for_means(assessment_tags, means, contribs):
  nitems = len(means)
  # # Create DataFrames for the mean values
  df = pd.DataFrame({
      'Assessment': assessment_tags,
      'MeanValue': means,
      'Category': ['Physical Health']*nitems, #+ ['Mental Health']*nitems,
      'Count': contribs #[len(first_assessments), len(last_assessments), len(first_assessments), len(last_assessments)]      
  })

  """Returns a chart for the given DataFrame."""
  chart = alt.Chart(df).mark_line(point=True).encode(
      x='Assessment',
      y='MeanValue',
      tooltip=['Assessment', 'MeanValue']
  ).properties(
      title='Mean Values of First and Last Physical Health Assessments'
  ).properties(
    width=600  # Specify the width here
  )

  # Add points to the line plot
  points = alt.Chart(df).mark_point(size=100).encode(
      x='Assessment',
      y='MeanValue',
      # color=alt.Color('Category', scale=alt.Scale(domain=['Physical Health', 'Mental Health'], range=['red', 'blue'])),
      color=alt.Color('Category', scale=alt.Scale(domain=['Physical Health'], range=['red'])),
      tooltip=['Assessment', 'MeanValue', 'Category', 'Count']
  ).properties(
      width=600  # Specify the width here
  )
  return chart + points

# # Create DataFrames for the mean values
# mean_physical_df = pd.DataFrame({
#     'Assessment': ['First', 'Last'],
#     'MeanValue': [mean_first_physical, mean_last_physical]
# })

# mean_mental_df = pd.DataFrame({
#     'Assessment': ['First', 'Last'],
#     'MeanValue': [mean_first_mental, mean_last_mental]
# })

# # Create the line plots
# chart_physical = alt.Chart(mean_physical_df).mark_line(point=True).encode(
#     x='Assessment',
#     y='MeanValue',
#     tooltip=['Assessment', 'MeanValue']
# ).properties(
#     title='Mean Values of First and Last Physical Health Assessments'
# )

# chart_mental = alt.Chart(mean_mental_df).mark_line(point=True).encode(
#     x='Assessment',
#     y='MeanValue',
#     tooltip=['Assessment', 'MeanValue']
# ).properties(
#     title='Mean Values of First and Last Mental Health Assessments'
# )

# # Concatenate the charts vertically
# chart = alt.hconcat(chart_physical, chart_mental)

# # Display the chart
# chart
