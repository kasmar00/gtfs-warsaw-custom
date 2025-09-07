all: feeds publish

feeds/warsaw-ferries/latest.zip:
	cd feeds/warsaw-ferries && zip -j ../warsaw-ferries/latest.zip *.txt

feeds/zabki/latest.zip:
	$(MAKE) -C feeds/zabki

feeds/minsk-maz/latest.zip:
	$(MAKE) -C feeds/minsk-maz

feeds/pgztppm/latest.zip:
	$(MAKE) -C feeds/pgztppm

publish:
	mkdir out
	rsync --recursive --include="latest.zip" --filter="-! */" feeds out

feeds: feeds/warsaw-ferries/latest.zip feeds/zabki/latest.zip feeds/minsk-maz/latest.zip feeds/pgztppm/latest.zip

clean:
	find . -name "*.zip" -type f -delete
	rm -rf out
	$(MAKE) -C feeds/zabki clean
	$(MAKE) -C feeds/minsk-maz clean
	$(MAKE) -C feeds/pgztppm clean
