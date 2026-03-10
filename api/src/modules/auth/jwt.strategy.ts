import { Injectable } from '@nestjs/common';
import { PassportStrategy } from '@nestjs/passport';
import { ExtractJwt, Strategy } from 'passport-jwt';

export interface JwtPayload {
    sub: string;
    username: string;
    role: string;
    permissions?: string[];
}

@Injectable()
export class JwtStrategy extends PassportStrategy(Strategy) {
    constructor() {
        super({
            jwtFromRequest: ExtractJwt.fromAuthHeaderAsBearerToken(),
            ignoreExpiration: false,
            secretOrKey: process.env.JWT_SECRET || 'changeme-jwt-secret',
        });
    }

    validate(payload: JwtPayload) {
        return {
            userId: payload.sub,
            username: payload.username,
            role: payload.role,
            permissions: payload.permissions ?? [],
        };
    }
}
