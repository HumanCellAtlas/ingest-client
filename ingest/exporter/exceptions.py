class BundleDSSError(Exception):
    """There was a failure in bundle creation in DSS."""


class BundleFileUploadError(Exception):
    """There was a failure in bundle file upload."""


class FileDSSError(Exception):
    """There was a failure in file creation in DSS."""


class InvalidBundleError(Exception):
    """There was a failure in bundle validation."""


class MultipleProjectsError(Exception):
    """A process should only have one project linked."""


class NoUploadAreaFoundError(Exception):
    """Export couldn't be as no upload area found"""
