"""Exceptions for the vSphere Control integration."""

from __future__ import annotations


class VSphereError(Exception):
    """Base exception for vSphere integration."""


class VSphereConnectionError(VSphereError):
    """Connection failed — timeout, SSL, network, session expired."""


class VSphereAuthError(VSphereError):
    """Authentication failed — invalid credentials, password expired, not licensed."""


class VSphereOperationError(VSphereError):
    """Operation failed — task error, permission denied, invalid state."""
