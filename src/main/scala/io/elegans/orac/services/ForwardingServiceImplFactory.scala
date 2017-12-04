package io.elegans.orac.services

/**
  * Created by Angelo Leto <angelo.leto@elegans.io> on 1/12/17.
  */

case class ForwardingImplementationNotFoundException(message: String = "", cause: Throwable = null)
  extends Exception(message, cause)

object SupportedForwardingServicesImpl extends Enumeration {
  type Permission = Value
  val csrec_0_4_1, unknown = Value
  def getValue(auth_method: String): SupportedForwardingServicesImpl.Value =
    values.find(_.toString == auth_method).getOrElse(SupportedForwardingServicesImpl.unknown)
}

object ForwardingServiceImplFactory {
  def apply(forwardingDestination: ForwardingDestination): AbstractForwardingImplService = {
    forwardingDestination.service_type match {
      case SupportedForwardingServicesImpl.csrec_0_4_1 =>
        new ForwardingService_CSREC_0_4_1(forwardingDestination)
      case _ =>
        throw ForwardingImplementationNotFoundException("Forwarder not supported: " +
          forwardingDestination.service_type)
    }
  }
}