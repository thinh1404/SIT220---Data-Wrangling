import jupytext
import os

# Get the directory containing the jupytext package
jupytext_dir = os.path.dirname(jupytext.__file__)

# Construct the full path to the executable
jupytext_executable = os.path.join(jupytext_dir, "jupytext")

print("Path to Jupytext executable:", jupytext_executable)