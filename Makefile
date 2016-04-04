install: 
	@./gradlew install
update:
	@./gradlew install --refresh-dependencies
test:
	@./gradlew test --tests Tester
adhoc:
	@./gradlew test --tests Adhoc
hamming:
	@./gradlew test --tests HammingNumbers
idea: 
	@./gradlew idea
doc:
	@./gradlew javadoc
clean: 
	@./gradlew clean 
