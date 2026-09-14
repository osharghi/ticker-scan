FROM public.ecr.aws/lambda/python:3.11

# Copy requirements.txt to the container
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy your script code into the task root
COPY lambda_function.py sp500_tickers.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler file name and function name
CMD [ "lambda_function.lambda_handler" ]