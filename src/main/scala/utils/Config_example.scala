package utils

object Config_example {
  // The local directory containing this repository
  val projectDir :String = "/path/to/project"
  // THe name of your bucket on AWS S3
  val s3bucketName :String = "taxi-dataset-antonioni-rubboli"
  // The path to the credentials file for AWS (if you follow instructions, this should not be updated)
  val credentialsPath :String = "/aws_credentials.txt"
}
