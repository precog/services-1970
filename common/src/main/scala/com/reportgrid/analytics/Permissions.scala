package com.reportgrid.analytics

/** Permissions dictate how a token may be used. Read permission allows
 * a token to read data. Write permission allows a token to write data.
 * Share permission allows a token to create new tokens with the same
 * or weaker level of permission.
 */
case class Permissions(read: Boolean, write: Boolean, share: Boolean) {
  /** Issues new permissions derived from this one. The new permissions 
   * cannot be broader than these permissions.
   */
  def issue(read: Boolean, write: Boolean, share: Boolean): Permissions = Permissions(
    read  = this.read  && read,
    write = this.write && write,
    share = this.share && share
  )
  
  /** Limits these permissions to the specified permissions. 
   */
  def limitTo(that: Permissions) = that.issue(read, write, share)
}

object Permissions {
  val All = Permissions(true, true, true)
}

