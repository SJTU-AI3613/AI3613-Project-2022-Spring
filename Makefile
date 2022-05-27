.PHONY: build clean format test grade submit-project1 docker-build docker-run

build:
	@mkdir -p build && cd build && cmake .. && make -j
	
clean:
	@rm -rf build

format:
	@find . \( -name "*.h" -o -iname "*.cc" \) |xargs clang-format -i -style=file

test:
	@cd build && ctest --out-on-failure

grade-project1:
	@bash scripts/grade.sh "project1"

grade-project2:
	@bash scripts/grade.sh "project2"

submit-project1:
	@git archive --format tar.gz --output "project1_submission.tar.gz" project1

submit-project2:
	@git archive --format tar.gz --output "project2_submission.tar.gz" project2

docker-build:
	@docker build . -t naivedb

docker-run:
	@docker run -it --rm -v `pwd`:`pwd` -w `pwd` naivedb