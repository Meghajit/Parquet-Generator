# Parquet Generator

This is a simple library which helps to generate parquet files containing the required data  and as per the schema.

## Design
The library consists of a single concrete class named `ParquetGenerator`. It has 3 methods with their contract like this:

    public class ParquetGenerator {
         public ParquetGenerator(String filePath, MessageType schema) throws IOException();
         public void writeToFile(SimpleGroup simpleGroup) throws IOException();
         public void closeWriter() throws IOException();
    }

* The constructor takes 2 arguments:
    * filePath: This is the required path to the output parquet file where the data needs to be written. The file will be generated automatically and need not be created prior.
    * schema: This is the schema of the parquet file denoted as a [MessageType](https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/schema/MessageType.java) object.
* The `writeToFile` method takes a single [SimpleGroup](https://github.com/apache/parquet-mr/blob/master/parquet-column/src/main/java/org/apache/parquet/example/data/simple/SimpleGroup.java) object. This object contains the entire data required to be written to the file.
* The `closeWriter` method should be invoked when the write job is done. This will close the open stream to the file and close the ParquetWriter.

## Usage

The library can be built into a jar and then imported as a dependency into the application. Publishing as a maven package hasn't been implemented yet.
Otherwise, the library also a single test class [ParquetGeneratorTest](https://github.com/Meghajit/Parquet-Generator/blob/291bfb59705cb21f87d498997fa4fbb8fa8f0a09/lib/src/test/java/parquet/generator/ParquetGeneratorTest.java) which can be utilised to generate the parquet file.

The test class has ample examples on how to generate different kinds of parquet schema, how to build simple group based on that schema and using the library to build a parquet file.
