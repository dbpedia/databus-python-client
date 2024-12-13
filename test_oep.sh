#!/usr/bin/env bash

databusclient deploy \
	--version-id "https://databus.openenergyplatform.org/koubaa/test_group/testArtifact/1.0-alpha/" \
	--title "Test Title" \
	--abstract "Test Abstract" \
	--description "Test Description" \
	--license-uri "http://dalicc.net/licenselibrary/AdaptivePublicLicense10" \
	--apikey "aef1a7d0-6038-4eef-a821-5cb1c538da06" \
	"https://raw.githubusercontent.com/dbpedia/databus/68f976e29e2db15472f1b664a6fd5807b88d1370/README.md"
