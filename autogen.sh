find | grep \\.c$ | sed s/^..//g > po/POTFILES.in
autoreconf -fi
