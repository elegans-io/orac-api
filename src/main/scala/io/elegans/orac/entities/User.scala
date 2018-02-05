package io.elegans.orac.entities

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 17/11/17.
  */

case class User(
                 id: String, /** user id */
                 password: String, /** user password */
                 salt: String, /** salt for password hashing */
                 permissions: Map[
                   String, /** index name */
                   Set[Permissions.Permission] /** permissions granted for the index */
                   ]
               )
