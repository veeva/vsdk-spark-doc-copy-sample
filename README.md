# Vault Java SDK - vsdk-spark-doc-copy-sample

**Please see the [project wiki](https://github.com/veeva/vsdk-spark-doc-copy-sample/wiki) for a detailed walkthrough.**

The vsdk-spark-doc-copy-sample project covers the use of Spark Messaging to copy a document from one vault (source) to another vault (target). The project will step through:

* Setup of the necessary Vault to Vault components
    * Vault _Connection_ records
    * Vault _Queues_ for the inbound and outbound Spark Messages
    * Various Vault components for the sample project
* Sample Code for:
    * Document Copy - when a document's lifecycle state changes to _approved_, a pending entry is placed in the _integration transactions_ record of the source vault and a Spark message is sent to the target vault to notify it the changes has occurred. This initiates a message processor in the target vault, which uses HTTP callouts to retrieve details of the pending _integration transaction_ and the subsequent source documents to be copied. The copy of the document is then created in the target vault, and the _integration transactions_ record is updated so the record isn't reprocessed.
	

**You will need two sandbox vaults to run through this project.**

## How to import

Import as a Maven project. This will automatically pull in the required Vault Java SDK dependencies. 

For Intellij this is done by:
- File -> Open -> Navigate to project folder -> Select the 'pom.xml' file -> Open as Project

For Eclipse this is done by:
- File -> Import -> Maven -> Existing Maven Projects -> Navigate to project folder -> Select the 'pom.xml' file
	    
## License

This code serves as an example and is not meant for production use.

Copyright 2020 Veeva Systems Inc.
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

