%if 0%{?rhel} && 0%{?rhel} <= 5
%{!?python_sitelib: %global python_sitelib %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib())")}
%{!?python_sitearch: %global python_sitearch %(%{__python} -c "from distutils.sysconfig import get_python_lib; print(get_python_lib(1))")}
%endif

Name:           swiftly
Version:        2.06
Release:        1
Summary:        Client for Swift

Group:          Development/Tools
License:        Unknown
URL:            https://github.com/gholt/swiftly
Source0:        %{name}-%{version}.tar.gz
BuildRoot:      %{_tmppath}/%{name}-%{version}-%{release}-root-%(%{__id_u} -n)

BuildRequires:  python-devel, python-setuptools

%description
Client for OpenStack Swift

%prep
%setup -q -n %{name}-%{version}

%build
%{__python} setup.py build

%install
rm -rf %{buildroot}
%{__python} setup.py install -O1 --skip-build --root %{buildroot}

%clean
rm -rf %{buildroot}

%files
%defattr(-,root,root,-)
%{_bindir}/%{name}
%{python_sitelib}/%{name}
%{python_sitelib}/%{name}-%{version}-*.egg-info

%changelog
* Tue Jun  6 2014 Gregory Holt <greg@brim.net> - 2.06-1
- Bug fixes; See http://gholt.github.io/swiftly/2.06/changelog.html
* Tue Apr 22 2014 Gregory Holt <greg@brim.net> - 2.04-1
- Bug fixes; See http://gholt.github.io/swiftly/2.04/changelog.html
* Sat Feb 15 2014 Gregory Holt <greg@brim.net> - 2.02-1
- Conf and bug fixes; See http://gholt.github.io/swiftly/2.02/changelog.html
* Sat Jan  4 2014 Gregory Holt <greg@brim.net> - 2.00-1
- Refactor; See http://gholt.github.io/swiftly/dev/changes_2_00.html
* Thu Nov 21 2013 Gregory Holt <greg@brim.net> - 1.12-1
- Bug fixes; updated ping; new trans command
* Sat Oct 19 2013 Gregory Holt <greg@brim.net> - 1.10-1
- Added new ping command
* Fri May 17 2013 Gregory Holt <greg@brim.net> - 1.8-1
- Releasing Swiftly 1.8
* Thu Mar 14 2013 Gregory Holt <gholt@rackspace.com> - 1.6-1
- Fix for change in swift's memcache client class
* Tue Dec  5 2012 Gregory Holt <gholt@rackspace.com> - 1.3-1
- working development release
* Tue Dec  5 2012 Gregory Holt <gholt@rackspace.com> - 1.2-1
- porting RPM packaging to swiftly repo itself
* Mon Oct 15 2012 Jeffrey Ness <jeffrey.ness@rackspace.com> - 1.0-1
- new package
