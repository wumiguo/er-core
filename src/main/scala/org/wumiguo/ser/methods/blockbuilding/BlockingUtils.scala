package org.wumiguo.ser.methods.blockbuilding

/**
 * Common methods for the different blocking techniques
 * @author Luca Gagliardelli
 * @since 2016/12/07
 */
object BlockingUtils {
  /** Defines the pattern used for tokenization */
  object TokenizerPattern {
    /** Split the token by underscore, whitespaces and punctuation */
    val DEFAULT_SPLITTING = "[\\W_]"
  }

  /**
   * Given a tuple (entity ID, [List of entity tokens])
   * produces a list of tuple (token, entityID)
    *
    * @param profileEntryKeys couple (entity ID, [List of entity keys])
   **/
  def associateKeysToProfileID(profileEntryKeys: (Int, Iterable[String])): Iterable[(String, Int)] = {
    val profileId = profileEntryKeys._1
    val keys = profileEntryKeys._2
    keys.map(key => (key, profileId))
  }

  /**
    * Used in the method that calculates the entropy of each block
    * @param profileEntryKeys couple (entity ID, [List of entity' tokens])
    * @return a list of (token, (profileID, [tokens hashes]))
    **/
  def associateKeysToProfileIdEntropy(profileEntryKeys: (Int, Iterable[String])): Iterable[(String, (Int, Iterable[Int]))] = {
    val profileId = profileEntryKeys._1
    val tokens = profileEntryKeys._2
    tokens.map(tokens => (tokens, (profileId, tokens.map(_.hashCode))))
  }
}
