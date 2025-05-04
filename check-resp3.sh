#!/bin/bash

echo -e "*2\r\n\$5\r\nHELLO\r\n\$1\r\n3\r\n*1\r\n\$4\r\nPING\r\n" | nc localhost 6379
