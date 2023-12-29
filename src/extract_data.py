import os

# Get the current working directory
current_directory = os.getcwd()
print("Current Directory:", current_directory)

# Get the parent directory
parent_directory = os.path.abspath(os.path.join(current_directory, os.pardir))
print("parent_directory:", parent_directory)
parent_directory

# Change the working directory to the parent directory
os.chdir(parent_directory)

# Display the updated current working directory
updated_directory = os.getcwd()
print("Updated Directory:", updated_directory)