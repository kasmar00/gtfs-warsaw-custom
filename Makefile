feeds/warsaw-water-tram/latest.zip:
	zip -r feeds/warsaw-water-tram/latest.zip feeds/warsaw-water-tram/*.txt

publish:
	mkdir out
	rsync --recursive --include="latest.zip" --filter="-! */" feeds out

feeds: feeds/warsaw-water-tram/latest.zip

all: feeds publish

clean:
	find . -name "*.zip" -type f -delete
	rm -rf out