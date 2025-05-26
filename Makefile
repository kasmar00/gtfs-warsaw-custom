feeds/warsaw-ferries/latest.zip:
	zip -r feeds/warsaw-ferries/latest.zip feeds/warsaw-ferries/*.txt

publish:
	mkdir out
	rsync --recursive --include="latest.zip" --filter="-! */" feeds out

feeds: feeds/warsaw-ferries/latest.zip

all: feeds publish

clean:
	find . -name "*.zip" -type f -delete
	rm -rf out