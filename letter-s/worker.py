def work_on_it(operation, operand):
    if (operation == 'prime'):
        return work_on_prime(operand)
    elif (operation == 'fibonacci'):
        return work_on_fibonacci(operand)


def work_on_prime(number):
    last_prime = 2

    for num in list(xrange(number + 1))[2:]:
        found_prime = True
        for previous_nums in list(xrange(num))[2:]:
            if (num % previous_nums == 0):
                found_prime = False
                break

        if (found_prime):
            last_prime = num

    print 'Greatest prime under %d: %d' % (number, last_prime)


def work_on_fibonacci(number):
    n_2 = 0
    n_1 = 1

    for num in list(xrange(number + 1))[2:]:
        n_2, n_1 = n_1, n_2 + n_1

    print 'Fibonacci number %d: %d' % (number, n_1)


if __name__ == "__main__":
    work_on_it('prime', 1000)
    work_on_it('fibonacci', 13)
