package org.hammerlab.spear

import com.foursquare.rogue.spindle.SpindleDatabaseService

trait HasDatabaseService {
  def db: SpindleDatabaseService
}
