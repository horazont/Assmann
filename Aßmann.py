#!/usr/bin/python3

import numpy as np

from MarkovSource import MarkovSource
from MarkovSource import ProbabilityMatrix

ALPH = [' '] + [chr(i) for i in range(ord('a'), ord('d')+1)]
ALPH = ['a', 'b', 'c']
N = len(ALPH)

ps = np.loadtxt('ps.txt')

init_state = np.zeros(N)
init_state[0] = 1
np.savetxt('init_state.txt', init_state)

source = MarkovSource(ALPH, init_state, ProbabilityMatrix(ps), 1)
for i in range(30):
    print(next(source.emit()), end='')
