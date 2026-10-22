package com.ecommerce.analytics

import org.scalatest.funsuite.AnyFunSuite

class TimeFeaturesSpec extends AnyFunSuite {

  test("exemple du sujet : lundi 1er juillet 2024 a 02h18") {
    assert(TimeFeatures.extract("20240701021822") ==
      Some(TimeFeatures(2, "Lundi", "Juillet", 0, "Night", 0)))
  }

  test("un samedi apres-midi est un week-end en heures de travail") {
    val f = TimeFeatures.extract("20240706120000").get
    assert(f.day_of_week == "Samedi" && f.is_weekend == 1 && f.day_period == "Afternoon" && f.is_working_hours == 1)
  }

  test("un dimanche est un week-end") {
    assert(TimeFeatures.extract("20240707090000").get.is_weekend == 1)
  }

  test("bornes des periodes de la journee") {
    assert(TimeFeatures.dayPeriod(5)  == "Night")
    assert(TimeFeatures.dayPeriod(6)  == "Morning")
    assert(TimeFeatures.dayPeriod(11) == "Morning")
    assert(TimeFeatures.dayPeriod(12) == "Afternoon")
    assert(TimeFeatures.dayPeriod(17) == "Afternoon")
    assert(TimeFeatures.dayPeriod(18) == "Evening")
    assert(TimeFeatures.dayPeriod(21) == "Evening")
    assert(TimeFeatures.dayPeriod(22) == "Night")
    assert(TimeFeatures.dayPeriod(23) == "Night")
  }

  test("bornes des heures de travail (9h a 17h inclus)") {
    assert(TimeFeatures.extract("20240702085959").get.is_working_hours == 0)
    assert(TimeFeatures.extract("20240702090000").get.is_working_hours == 1)
    assert(TimeFeatures.extract("20240702173000").get.is_working_hours == 1)
    assert(TimeFeatures.extract("20240702180000").get.is_working_hours == 0)
  }

  test("les chaines nulles, vides ou mal formees donnent None sans exception") {
    val invalides = Seq(null, "", "   ", "NA", "20240616", "2024061612345", "abcdefghijklmn",
      "20241301000000", "20240230101010", "20240101250000")
    invalides.foreach(s => assert(TimeFeatures.extract(s).isEmpty, s"devrait etre invalide : $s"))
  }
}
