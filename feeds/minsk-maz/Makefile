latest.zip: .venv
	. .venv/bin/activate; python3 -m minskmaz_gtfs

.venv: .venv/touchfile

.venv/touchfile: requirements.txt
	test -d venv || python3 -m venv .venv
	. .venv/bin/activate; pip install -Ur requirements.txt
	touch .venv/touchfile

clean:
	rm -rf _impuls_workspace
	rm latest.zip