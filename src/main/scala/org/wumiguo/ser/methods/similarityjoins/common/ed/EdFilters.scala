package org.wumiguo.ser.methods.similarityjoins.common.ed

/**
 * @author levinliu
 *         Created on 2020/6/11
 *         (Change file header on Settings -> Editor -> File and Code Templates)
 */
object EdFilters {

  def getPrefixLen(qGramLen: Int, threshold: Int): Int = {
    qGramLen * threshold + 1
  }

  /**
   * compare the d1 & d2 and return the status(pass or mismatch)
   * pass when 2 array has enough q-gram in common based on given threshold
   * @param d1
   * @param d2
   * @param qgramLength
   * @param threshold
   * @return
   */
  def commonFilter(d1: Array[(Int, Int)], d2: Array[(Int, Int)], qgramLength: Int, threshold: Int): Boolean = {
    var pass = true
    val minCommon = (Math.max(d1.length, d2.length) - qgramLength + 1) - (qgramLength * threshold)
    if (minCommon > 0) {
      var i = 0
      var j = 0
      var common = 0
      var continue = true

      while (i < d1.length && j < d2.length && common < minCommon && continue) {
        if (d1(i)._1 < d2(j)._1) {
          //move forward d1
          i += 1
        }
        else if (d1(i)._1 > d2(j)._1) {
          //move forward d2
          j += 1
        }
        else {
          do {
            //Se i due q-grammi hanno una distanza inferiore alla soglia allora lo conto
            if (math.abs(d1(i)._2 - d2(j)._2) <= threshold) {
              common += 1
            }

            //Con questa condizione posso verificare se le chance che mi rimangono sono sufficienti
            //per poter raggiungere l'overlap richiesto
            continue = (math.min(d1.length - i, d2.length - j) + common) >= minCommon

            //I qgrammi sono uguali, ora devo verificare le posizioni, a me servono nella stessa posizione!
            if (d1(i)._2 < d2(j)._2) {
              //Se la posizione di d1 è inferiore rispetto a quella di d2, allora muovo avanti d1
              i += 1
            }
            else if (d1(i)._2 > d2(j)._2) {
              //Se la posizione di d1 è maggiore di d2, allora muovo avanti d2
              j += 1
            }
            else {
              //Le posizioni sono uguali, muovo avanti entrambi
              i += 1
              j += 1
            }
          } while (i < d1.length && j < d2.length && common < minCommon && continue && d1(i)._1 == d2(j)._1)
        }
      }

      pass = common >= minCommon
    }
    pass
  }

  /**
   * Implementa il common filter partendo dai token comuni già visti nel prefisso
   * restituisce true se i due documenti hanno sufficienti q-grammi in comune per poter passare l'edit distance
   * richiesta
   **/
  def commonFilterAfterPrefix(d1: Array[(Int, Int)], d2: Array[(Int, Int)], qgramLength: Int, threshold: Int, commonPrefixQgrams: Int, d1StartPos: Int, d2StartPos: Int): Boolean = {
    var pass = true
    val minCommon = (Math.max(d1.length, d2.length) - qgramLength + 1) - (qgramLength * threshold)
    if (minCommon > 0) {
      var i = d1StartPos
      var j = d2StartPos
      var common = commonPrefixQgrams
      var continue = true

      while (i < d1.length && j < d2.length && common < minCommon && continue) {
        if (d1(i)._1 < d2(j)._1) {
          //Prima guardo il qgramma, se il qgramma di d1 è minore di quello di d2, allora muovo avanti d1
          i += 1
        }
        else if (d1(i)._1 > d2(j)._1) {
          //Se il qgrama di d1 è maggiore di quello di d2, allora muovo avanti d2
          j += 1
        }
        else {
          //Ripeto questo blocco finché i q-grammi sono uguali
          do {
            //Se i due q-grammi hanno una distanza inferiore alla soglia allora lo conto
            if (math.abs(d1(i)._2 - d2(j)._2) <= threshold) {
              common += 1
            }

            //Con questa condizione posso verificare se le chance che mi rimangono sono sufficienti
            //per poter raggiungere l'overlap richiesto
            continue = (math.min(d1.length - i, d2.length - j) + common) >= minCommon

            //I qgrammi sono uguali, ora devo verificare le posizioni, a me servono nella stessa posizione!
            if (d1(i)._2 < d2(j)._2) {
              //Se la posizione di d1 è inferiore rispetto a quella di d2, allora muovo avanti d1
              i += 1
            }
            else if (d1(i)._2 > d2(j)._2) {
              //Se la posizione di d1 è maggiore di d2, allora muovo avanti d2
              j += 1
            }
            else {
              //Le posizioni sono uguali, muovo avanti entrambi
              i += 1
              j += 1
            }
          } while (i < d1.length && j < d2.length && common < minCommon && continue && d1(i)._1 == d2(j)._1)
        }
      }

      pass = common >= minCommon
    }
    pass
  }
}
