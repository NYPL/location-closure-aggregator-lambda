#!/bin/zsh

pip install --target ./package -r requirements.txt
cd package
zip -r ../deployment-package.zip .
cd ..
zip deployment-package.zip lambda_function.py
zip deployment-package.zip query_helper.py