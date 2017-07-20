FLAGS=`root-config --libs --cflags` -shared -O3 -fPIC

libCalibrate.so: calibrate.cpp
	g++ $(FLAGS) calibrate.cpp -o libCalibrate.so

.PHONY: clean
clean:
	rm libCalibrate.so
