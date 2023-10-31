import pandas as pd
import os
import numpy as np
from datetime import timedelta

# this is still a test
# Specify the folder path where your CSV files are located
folder_path = "data"

# Specify the file names for your two CSV files
task_file = "TasksAlpha_all.csv"
toggl_file = "Toggl_time_entries_2023-10-09_to_2023-10-22.csv"

# Construct the full file paths
task_file_path = os.path.join(folder_path, task_file)
toggl_file_path = os.path.join(folder_path, toggl_file)

# Read the CSV files into dataframes with specific names
tasks = pd.read_csv(task_file_path)
toggl = pd.read_csv(toggl_file_path)

# Select subset of Tasks attributes
tasksSimple = pd.DataFrame(tasks[['taskID', 'name','status','expHours', 'mnemonic', 'assignee']])
#print(tasksSimple.head())

# Function to convert HH:MM:SS to hours
def convert_to_hours(duration_str):
    duration = pd.to_timedelta(duration_str)
    hours = duration.total_seconds() / 3600
    return hours

# Function to extract values based on conditions
def extract_task_value(task):
    if pd.isna(task) or str(task).strip().lower() == 'nan':
        return 'N/A'
    elif str(task).startswith('TS-'):
        return str(task).split(' ')[0]
    else:
        return str(task)
    
# Convert the "Task" column to string
toggl['Task'] = toggl['Task'].astype(str)

# Apply the function to the "Duration" column and store the result in a new column "Duration (hours)"
toggl['Duration_hours'] = toggl['Duration'].apply(convert_to_hours)

# Apply the function to create the new column 'taskID'
toggl['taskID'] = toggl['Task'].apply(extract_task_value)

print(toggl.head(10))

# Aggregate toggl entries per project and task only
togglSum = toggl.groupby(['Project', 'taskID']).agg(
    Duration_hours=('Duration_hours','sum')
).reset_index()


# Assuming you have "taskid" in both dataframes "tasks" and "toggl"
# Merge the dataframes based on the "taskid" column
merged_dataframe = togglSum.merge(tasksSimple, on='taskID', how='inner')

# The merged_dataframe will contain all rows where "taskid" matches in both dataframes
#print(merged_dataframe.head(100))

# Group by 'taskID' and sum 'expHours' and 'Duration_hours' for each taskID
summarized_dataframe = merged_dataframe.groupby('taskID').agg(
    planned=('expHours', 'sum'),
    realized=('Duration_hours', 'sum')
).reset_index()

# Calculate the difference between 'expHours' and 'Duration_hours'
summarized_dataframe['difference'] =  summarized_dataframe['realized'] - summarized_dataframe['planned']

# Merge the "summarized_dataframe" with the "task" dataframe based on 'taskID'
summarized_dataframe = summarized_dataframe.merge(tasksSimple[['taskID', 'name', 'mnemonic']], on='taskID', how='left')

summarized_dataframe.to_csv('planVsRealized.csv')
print(summarized_dataframe.head())