package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 20/11/17.
  */

case class UserUpdate(
                 password: Option[String] = Option.empty, /** user password */
                 salt: Option[String] = Option.empty, /** salt for password hashing */
                 permissions: Option[
                   Map[
                     String, /** index name */
                     Set[Permissions.Value] /** permissions granted for the index */
                     ]
                   ] = Option.empty
               )