import pandas as pd
import altair as alt

COLOR_RANGE = ['red', 'blue', 'black', 'green', 'yellow', 'orange']

def get_chart_for_qna_list(question_list, answers_df, title):
  
  nitems = len(question_list)
  
  # df = pd.DataFrame(answer_list)
  # df  
  
  chart = alt.Chart(answers_df).mark_line().encode(
      x='Assessment Number',
      y='Average',
      color='Question',
      # tooltip=['Question', 'Assessment Number', 'Average']
  ).properties(
        title=title
    ).properties(
      width=600  # Specify the width here
    )

  # Add points to the line plot
  points = alt.Chart(answers_df).mark_point(size=100).encode(
      x='Assessment Number',
      y='Average',
      color=alt.Color('Question', scale=alt.Scale(domain=question_list, range=COLOR_RANGE[:nitems])),
      tooltip=['Assessment Number', 'Average', 'Question', '# Contributions']
  ).properties(
      width=600  # Specify the width here
  )
  return chart, points
  # chart + points

# # Concatenate the charts vertically
# chart = alt.hconcat(chart_physical, chart_mental)

# # Display the chart
# chart
