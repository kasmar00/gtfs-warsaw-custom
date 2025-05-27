all: feeds publish

feeds/warsaw-ferries/latest.zip:
	zip -r feeds/warsaw-ferries/latest.zip feeds/warsaw-ferries/*.txt

feeds/zabki/latest.zip:
	$(MAKE) -C feeds/zabki

publish:
	mkdir out
	rsync --recursive --include="latest.zip" --filter="-! */" feeds out

feeds: feeds/warsaw-ferries/latest.zip feeds/zabki/latest.zip

clean:
	find . -name "*.zip" -type f -delete
	rm -rf out
	$(MAKE) -C feeds/zabki clean
