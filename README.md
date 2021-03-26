# Beam-Tutorial




#Run the application to calculate the marks of each candidate
```maven
mvn compile exec:java -Dexec.mainClass=com.amiown.beam.tutorial.TotalScoreComputation -Dexec.args="--outputFile=src/main/resources/sink/student_total_scores.csv --inputFile=src/main/resources/source/student_scores.csv"
```