@echo off
echo Setting up git configuration...
git config user.name "IRANNA-BJ"
git config user.email "irannajangannavar00@gmail.com"

echo Creating commit...
git commit -m "Initial commit: Production-ready COVID-19 ETL Pipeline"

echo Setting up remote repository...
git branch -M main
git remote add origin https://github.com/IRANNA-BJ/covid19-etl-pipeline.git

echo Pushing to GitHub...
git push -u origin main

echo Done! Your project has been pushed to GitHub.
pause
