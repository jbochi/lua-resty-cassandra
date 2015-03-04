.PHONY: test coverage lint clean

clean:
	@rm -f luacov.*

test:
	@busted spec/

coverage: clean
	@busted --coverage spec/
	@luacov cassandra

lint:
	@luacheck cassandra*.rockspec --globals ngx
