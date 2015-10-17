

all:
	python sync_ghs.py -f test_data/reader/input.txt

clean_logs:
	\rm -rf logs/*.log

clean:
	\rm -rf *.pyc
