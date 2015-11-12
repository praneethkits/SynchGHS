

all:
	python sync_ghs.py -f test_data/reader/input.txt

sample3:
	python sync_ghs.py -f test_data/reader/sample3.txt

sample4:
	python sync_ghs.py -f test_data/reader/sample4.txt

clean_logs:
	\rm -rf logs/*.log

clean:
	\rm -rf *.pyc
