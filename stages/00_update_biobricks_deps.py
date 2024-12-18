import os
import shutil
import subprocess
import urllib3

# Clean up and initialize biobricks
shutil.rmtree('.bb', ignore_errors=True)
subprocess.run('biobricks init && biobricks add ecotox', shell=True)
