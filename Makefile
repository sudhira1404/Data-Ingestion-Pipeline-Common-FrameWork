local-only:
	rm -rfd mdf-api-kelsa-dip
	cp application-local.yml src/main/resources/application.yml
	cp ads.properties src/main/resources/ads.properties
	./gradlew clean build distTar
	tar -xvf build/distributions/mdf-api-kelsa-dip.tar
	mkdir -p mdf-api-kelsa-dip/resources/main
	mdf-api-kelsa-dip/bin/mdf-api-kelsa-dip

tapctl-restart:
	tapctl stop simulator
	colima stop
	colima start
	tapctl start simulator --detach

tapctl-start:
	colima start
	tapctl start simulator --detach

tapctl-stop:
	tapctl stop simulator
	colima stop

tapctl-test:
	rm -rfd mdf-api-kelsa-dip
	rm -f src/main/resources/application.yml
	./gradlew clean build distTar
	docker build -t mdf-api-kelsa-dip:local .
	tapctl start cluster marketingingestpipelinesv2 mdf-api-kelsa-dip:local -e dev -p 8080:8080 -V "$(pwd):/data1" -v