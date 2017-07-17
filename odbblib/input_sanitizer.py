"""File with routines to ensure that the input obtained from users will convert
correctly, satisfy the correct bounds, etc"""
import sys


def test_bounds(value, kwargs):
    """Function to test if a numeric type falls within a set of bounds that may
    or may not be present

    Parameters
    ----------
    value : number
        The number to be bounds tested
    kwargs : dictionary
        The keyword arguments dictionary that may or may not contain inclusive
        or exclusive lower or upper bounds

    Returns
    -------
    good : bool
        True if value was within the provided bounds, false otherwise
    """
    # test either type of lower bound
    if "inclusive_lower_bound" in kwargs:
        if kwargs["inclusive_lower_bound"] > value:
            print value, "is below the inclusive lower bound of",\
                kwargs["inclusive_lower_bound"]
            return False
    elif "exclusive_lower_bound" in kwargs:
        if kwargs["exclusive_lower_bound"] >= value:
            print value, "is below the exclusive lower bound of",\
                kwargs["exclusive_lower_bound"]
            return False
    # test either type of upper bound
    if "inclusive_upper_bound" in kwargs:
        if kwargs["inclusive_upper_bound"] < value:
            print value, "is above the inclusive upper bound of",\
                kwargs["inclusive_upper_bound"]
            return False
    elif "exclusive_upper_bound" in kwargs:
        if kwargs["exclusive_upper_bound"] <= value:
            print value, "is above the exclusive upper bound of",\
                kwargs["exclusive_upper_bound"]
            return False
    return True


def get_float(prompt, **kwargs):
    """Function to get floating point numbers from the command line

    Parameters
    ----------
    prompt : str
        The string representing the prompt to be given when getting the input
    inclusive_lower_bound : float
        The minimum value that the input value must be greater than or equal to
        cannot be set at the same time as exclusive_lower_bound
    exclusive_lower_bound : float
        The minimum value that the input value must be greater than
        cannot be set at the same time as inclusive_lower_bound
    inclusive_upper_bound : float
        The maximum value that the input value must be less than or equal to
        cannot be set at the same time as exclusive_upper_bound
    exclusive_upper_bound : float
        The maximum value that the input value must be less than
        cannot be set at the same time as inclusive_upper_bound
    default_value : float
        The value that will be returned if the user simply presses enter
        If this is not set then the user pressing enter will be ignored

    Returns
    -------
    value : float
        The value obtained and converted from the command line
    """
    if "inclusive_lower_bound" in kwargs and "exclusive_lower_bound" in kwargs:
        raise ValueError("Cannot set inclusive *and* exclusive lower bounds"
                         " simultaneously")
    if "inclusive_upper_bound" in kwargs and "exclusive_upper_bound" in kwargs:
        raise ValueError("Cannot set inclusive *and* exclusive upper bounds"
                         " simultaneously")
    successful_input = False
    while not successful_input:
        # first get whatever the user types
        in_str = ""
        try:
            if "default_value" in kwargs:
                pstr = "{0:s} ({1:f})?> ".format(prompt,
                                                 kwargs["default_value"])
                in_str = raw_input(pstr)
            else:
                in_str = raw_input("{0:s}?> ".format(prompt))
        except EOFError:
            print "Got end of file exitting"
            sys.exit()
        # now try to convert it to float
        value = None
        if len(in_str) == 0:
            if "default_value" in kwargs:
                return kwargs["default_value"]
            else:
                print "There is no default value, you must enter a value"
                continue
        try:
            value = float(in_str)
        except ValueError:
            print in_str, "cannot be converted to a float, try again"
            continue
        # now test the bounds
        if test_bounds(value, kwargs):
            successful_input = True
            return value


def get_int(prompt, **kwargs):
    """Function to get integers from the command line

    Parameters
    ----------
    prompt : str
        The string representing the prompt to be given when getting the input
    inclusive_lower_bound : int
        The minimum value that the input value must be greater than or equal to
        cannot be set at the same time as exclusive_lower_bound
    exclusive_lower_bound : int
        The minimum value that the input value must be greater than
        cannot be set at the same time as inclusive_lower_bound
    inclusive_upper_bound : int
        The maximum value that the input value must be less than or equal to
        cannot be set at the same time as exclusive_upper_bound
    exclusive_upper_bound : int
        The maximum value that the input value must be less than
        cannot be set at the same time as inclusive_upper_bound
    default_value : int
        The value that will be returned if the user simply presses enter
        If this is not set then the user pressing enter will be ignored

    Returns
    -------
    value : int
        The value obtained and converted from the command line
    """
    if "inclusive_lower_bound" in kwargs and "exclusive_lower_bound" in kwargs:
        raise ValueError("Cannot set inclusive *and* exclusive lower bounds"
                         " simultaneously")
    if "inclusive_upper_bound" in kwargs and "exclusive_upper_bound" in kwargs:
        raise ValueError("Cannot set inclusive *and* exclusive upper bounds"
                         " simultaneously")
    successful_input = False
    while not successful_input:
        # first get whatever the user types
        in_str = ""
        try:
            if "default_value" in kwargs:
                pstr = "{0:s} ({1:d})?> ".format(prompt,
                                                 kwargs["default_value"])
                in_str = raw_input(pstr)
            else:
                in_str = raw_input("{0:s}?> ".format(prompt))
        except EOFError:
            print "Got end of file exitting"
            sys.exit()
        # now try to convert it to float
        value = None
        if len(in_str) == 0:
            if "default_value" in kwargs:
                return kwargs["default_value"]
            else:
                print "There is no default value, you must enter a value"
                continue
        try:
            value = int(in_str)
        except ValueError:
            print in_str, "cannot be converted to an int, try again"
            continue
        # now test the bounds
        if test_bounds(value, kwargs):
            successful_input = True
            return value

def get_bool(prompt, default_value=None):
    """Function to get a boolean from the command line

    Parameters
    ----------
    prompt : str
        The string representing the prompt to be given when getting the input
    default_value : bool
        The value that will be returned if the user simply presses enter
        If this is not set then the user pressing enter will be ignored

    Returns
    -------
    value : bool
        The value obtained and converted from the command line
    """
    successful_input = False
    while not successful_input:
        # first get whatever the user types
        in_str = ""
        try:
            if default_value is not None:
                def_str = ("T/f" if default_value else "t/F")
                pstr = "{0:s} ({1:s})?> ".format(prompt, def_str)
                in_str = raw_input(pstr)
            else:
                in_str = raw_input("{0:s}?> ".format(prompt))
        except EOFError:
            print "Got end of file exitting"
            sys.exit()
        # now try to convert it to float
        value = None
        if len(in_str) == 0:
            if default_value is not None:
                return default_value
            else:
                print "There is no default value, you must enter a value"
                continue
        if in_str.lower() in ['t', 'true']:
            value = True
        elif in_str.lower() in ['f', 'false']:
            value = False
        else:
            print in_str, "cannot be converted to a boolean, try 'f' or 't'"
            continue
        successful_input = True
        return value


def get_yes_no(prompt, default_value=None):
    """Function to get a boolean from the command line

    Parameters
    ----------
    prompt : str
        The string representing the prompt to be given when getting the input
    default_value : bool
        The value that will be returned if the user simply presses enter
        If this is not set then the user pressing enter will be ignored

    Returns
    -------
    value : bool
        true if yes, false if no
    """
    successful_input = False
    while not successful_input:
        # first get whatever the user types
        in_str = ""
        try:
            if default_value is not None:
                def_str = ("Y/n" if default_value else "y/N")
                pstr = "{0:s} ({1:s})?> ".format(prompt, def_str)
                in_str = raw_input(pstr)
            else:
                in_str = raw_input("{0:s}?> ".format(prompt))
        except EOFError:
            print "Got end of file exitting"
            sys.exit()
        # now try to convert it to float
        value = None
        if len(in_str) == 0:
            if default_value is not None:
                return default_value
            else:
                print "There is no default value, you must enter a value"
                continue
        if in_str.lower() in ['y', 'yes']:
            value = True
        elif in_str.lower() in ['n', 'no']:
            value = False
        else:
            print in_str, "cannot be converted to a yes or no, try 'y' or 'n'"
            continue
        successful_input = True
        return value


def get_str(prompt, default_value=None):
    """Function to get a boolean from the command line

    Parameters
    ----------
    prompt : str
        The string representing the prompt to be given when getting the input
    default_value : bool
        The value that will be returned if the user simply presses enter
        If this is not set then the user pressing enter will be ignored

    Returns
    -------
    value : bool
        true if yes, false if no
    """
    successful_input = False
    while not successful_input:
        # first get whatever the user types
        in_str = ""
        try:
            if default_value is not None:
                pstr = "{0:s} ({1:s})?> ".format(prompt, default_value)
                in_str = raw_input(pstr)
            else:
                in_str = raw_input("{0:s}?> ".format(prompt))
        except EOFError:
            print "Got end of file exitting"
            sys.exit()
        # now try to convert it to float
        value = None
        if len(in_str) == 0:
            if default_value is not None:
                return default_value
            else:
                print "There is no default value, you must enter a value"
                continue
        value = in_str
        successful_input = True
        return value
