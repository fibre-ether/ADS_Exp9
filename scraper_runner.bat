:start
echo running the scraper
python colab.py
echo done running the scraper, waiting 5 seconds
ping -n 6 127.0.0.1>NUL
goto start
