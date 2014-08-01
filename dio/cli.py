import sys
import dio

default_in  = dio.in_json
default_out = dio.out_json(out=sys.stdout)
default_err = dio.out_json(out=sys.stderr)

def cli(p):
	default_in(p, out=default_out, err=default_err)


if __name__=='__main__':
	pass
