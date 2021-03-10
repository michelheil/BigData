import AppErrorReload.csvToMetaKafkaMessages
import org.scalatest.MustMatchers
import org.scalatest.WordSpec

class AppErrorReloadTest extends WordSpec
  with MustMatchers {

  "csvToMetaKafkaMessages" can {
    "parse a csv file and store its content into MetaKafkaMessages" in {
      // Given
      val fileName = "error-test_positive.csv"

      // When
      val actual = csvToMetaKafkaMessages(fileName).toSeq

      // Then
      actual.size must be(3)
      actual.head.appName must be("app1")
      actual.head.topicName must be("topic1")
      actual.head.offset must be(1L)
    }

    "parse a csv file with spaces and store its content into MetaKafkaMessages" in {
      // Given
      val fileName = "error-test_spaces.csv"

      // When
      val actual = csvToMetaKafkaMessages(fileName).toSeq

      // Then
      actual.size must be(2)
      actual.head.appName must be("app1")
      actual.head.topicName must be("topic1")
      actual.head.offset must be(1L)
      actual.tail.head.appName must be("app2")
      actual.tail.head.topicName must be("topic2")
      actual.tail.head.partition must be(4)
    }

    "throw ArrayIndexOutOfBoundsException if not enough columns are provided" in {
      // Given
      val fileName = "error-test_missing.csv"

      // When
      val e = intercept[ArrayIndexOutOfBoundsException] {
        csvToMetaKafkaMessages(fileName).toSeq
      }

      // Then
      e.getMessage must be ("3")
    }

    "throw NumberFormatException if the values in csv files have wrong format" in {
      // Given
      val fileName = "error-test_wrong_format.csv"

      // When
      val e = intercept[NumberFormatException] {
        csvToMetaKafkaMessages(fileName).toSeq
      }

      // Then
      e.getMessage must include ("foo")
    }

    "handle too many columns in given csv file if first four values are valid" in {
      // Given
      val fileName = "error-test_too_many.csv"

      // When
      val actual = csvToMetaKafkaMessages(fileName).toSeq

      // Then
      actual.size must be(2)
      actual.head.appName must be("app1")
      actual.head.topicName must be("topic1")
      actual.head.offset must be(3L)
      actual.tail.head.appName must be("app2")
      actual.tail.head.topicName must be("topic2")
      actual.tail.head.partition must be(4)
    }

  }

}
