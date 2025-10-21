#!/usr/bin/env bash

databusclient deploy \
	--version-id "https://d8lr.tools.dbpedia.org/hopver/testGroup/testArtifact/1.0-alpha/" \
	--title "Test Title" \
	--abstract "Test Abstract" \
	--description "Test Description" \
	--license-uri "https://dalicc.net/licenselibrary/Apache-2.0" \
	--apikey "f67f582e-bb33-4e82-ba1a-cbaa750be278" \
	"https://raw.githubusercontent.com/dbpedia/databus/68f976e29e2db15472f1b664a6fd5807b88d1370/README.md"
