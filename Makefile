REBAR = rebar
ERL   = erl


.PHONY: deps

all: deps compile

compile:
	$(REBAR) compile

deps:
	$(REBAR) get-deps

clean:
	$(REBAR) clean

ct: compile
	$(REBAR) ct skip_deps=true -v
