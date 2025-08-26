import math
import numpy as np

g = 9.81

def stokes_velocity(d, rho_p, rho_f, mu):
    return ((rho_p - rho_f) * g * d**2) / (18.0 * mu)

def reynolds_number(v, d, rho_f, mu):
    if v <= 0.0: return 0.0
    return rho_f * v * d / mu

def cd_schiller_naumann(Re):
    if Re <= 0: return 1e9
    if Re < 1000.0:
        return 24.0/Re * (1.0 + 0.15 * (Re**0.687))
    return 0.44

def terminal_velocity_iterative(d, rho_p, rho_f, mu, tol=1e-9, max_iter=200):
    v = max(stokes_velocity(d, rho_p, rho_f, mu), 1e-12)
    for _ in range(max_iter):
        Re = reynolds_number(v, d, rho_f, mu)
        Cd = cd_schiller_naumann(Re)
        v_new = math.sqrt( (4.0/3.0) * ((rho_p - rho_f) * g * d) / (rho_f * Cd) )
        if abs(v_new - v) <= tol * max(1.0, v_new):
            return v_new
        v = v_new
    return v

def terminal_velocity(d, rho_p, rho_f, mu, re_threshold=0.5):
    v_stokes = stokes_velocity(d, rho_p, rho_f, mu)
    Re_stokes = reynolds_number(v_stokes, d, rho_f, mu)
    if Re_stokes <= re_threshold:
        return v_stokes
    return terminal_velocity_iterative(d, rho_p, rho_f, mu)

def distance_over_time(v_term, times, start_depth, bottom_depth):
    distances = v_term * np.array(times)
    max_distance = max(0.0, bottom_depth - start_depth)
    return np.clip(distances, 0.0, max_distance)
