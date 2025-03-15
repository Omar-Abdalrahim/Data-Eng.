FROM python
RUN pip install pandas


COPY hello.py /usr/src/app/hello.py

# Set the entrypoint to execute the script
ENTRYPOINT ["python", "/usr/src/app/hello.py"]

